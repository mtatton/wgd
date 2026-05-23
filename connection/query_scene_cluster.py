#!/usr/bin/env python3
"""Read-only clustered WGD scene-construction queries."""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from decimal import Decimal
import json
from pathlib import Path
import sys
from typing import Any, Iterable, Sequence


ROOT = Path(__file__).resolve().parents[2]
REPLICATION_DIR = ROOT / "wgd" / "replication"
if str(REPLICATION_DIR) not in sys.path:
    sys.path.insert(0, str(REPLICATION_DIR))

from wgd_replication import (  # noqa: E402
    DEFAULT_CONFIG,
    ReplConfig,
    ReplError,
    connect,
    load_repl_config,
)


DEFAULT_CONFIG_PATH = DEFAULT_CONFIG.with_name("nodes.ini")
DEFAULT_TOP_CLUSTERS = 5
DEFAULT_TIMEOUT = 10
BUNDLE_LIMITS = {
    "objects": 8,
    "cameras": 5,
    "lights": 12,
    "materials": 12,
    "spatial_nodes": 25,
    "spatial_edges": 40,
    "cluster_members": 30,
}
CORE_TABLES = (
    "tmp_scenes",
    "tmp_descriptions",
    "tmp_objects",
    "tmp_povs",
    "tmp_materials",
    "tmp_cameras",
    "tmp_lights",
    "tmp_spatial_nodes",
    "tmp_spatial_edges",
    "tmp_spatial_cells",
    "tmp_graph_clusters",
    "tmp_graph_cluster_members",
)


class ClusterQueryError(RuntimeError):
    """Raised when a clustered query cannot be completed."""


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Query the WGD SQL Server cluster for scene-construction evidence.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG_PATH,
        help=f"WGD replication nodes config (default: {DEFAULT_CONFIG_PATH})",
    )
    parser.add_argument(
        "--scene-id",
        help="canonical scene id or scene key to inspect; defaults to the publisher's strongest cluster scene",
    )
    parser.add_argument(
        "--use-cluster",
        action="store_true",
        help="query all configured WGD cluster nodes with one worker per node; default is publisher-only",
    )
    parser.add_argument(
        "--top-clusters",
        type=positive_int,
        default=DEFAULT_TOP_CLUSTERS,
        help=f"top graph clusters to return per node (default: {DEFAULT_TOP_CLUSTERS})",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="output format (default: text)",
    )
    parser.add_argument(
        "--timeout",
        type=positive_int,
        default=DEFAULT_TIMEOUT,
        help=f"connection timeout in seconds (default: {DEFAULT_TIMEOUT})",
    )
    return parser.parse_args(argv)


def positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be an integer") from exc
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return parsed


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        config = load_repl_config(args.config)
        report = collect_cluster_report(
            config,
            requested_scene_id=args.scene_id,
            use_cluster=args.use_cluster,
            top_clusters=args.top_clusters,
            timeout=args.timeout,
        )
    except (ReplError, ClusterQueryError) as exc:
        print(f"wgd-scene-cluster-query: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:
        print(f"wgd-scene-cluster-query: {friendly_error(exc)}", file=sys.stderr)
        return 1

    print(format_report(report, args.format))
    return 0 if report["comparison"]["ok"] else 2


def collect_cluster_report(
    config: ReplConfig,
    *,
    requested_scene_id: str | None,
    use_cluster: bool,
    top_clusters: int,
    timeout: int,
) -> dict[str, Any]:
    nodes = target_nodes(config, use_cluster=use_cluster)
    worker_count = len(nodes) or 1
    first_pass = run_parallel(
        nodes,
        lambda node: collect_node_anchor_report(config, node.name, top_clusters=top_clusters, timeout=timeout),
        worker_count=worker_count,
    )
    publisher_report = first_pass.get(config.publisher)
    scene_id, cluster_key = select_scene_and_cluster(publisher_report, requested_scene_id)

    second_pass = run_parallel(
        nodes,
        lambda node: collect_node_bundle_report(
            config,
            node.name,
            scene_id=scene_id,
            cluster_key=cluster_key,
            timeout=timeout,
        ),
        worker_count=worker_count,
    )
    if cluster_key is None:
        publisher_bundle = (second_pass.get(config.publisher) or {}).get("bundle") or {}
        publisher_cluster = publisher_bundle.get("anchor_cluster") or {}
        cluster_key = publisher_cluster.get("cluster_key")
    by_node: list[dict[str, Any]] = []
    for node in nodes:
        anchor = first_pass[node.name]
        bundle = second_pass[node.name]
        merged = dict(anchor)
        if bundle.get("ok"):
            merged["selected_scene_id"] = scene_id
            merged["selected_cluster_key"] = cluster_key
            merged["bundle"] = bundle["bundle"]
        elif anchor.get("ok"):
            merged["ok"] = False
            merged["error"] = bundle.get("error", "bundle query failed")
        by_node.append(merged)

    comparison = compare_nodes(config, by_node, selected_scene_id=scene_id, selected_cluster_key=cluster_key)
    return {
        "database": config.database,
        "schema": config.schema,
        "table_prefix": "tmp_",
        "use_cluster": use_cluster,
        "worker_count": worker_count,
        "queried_nodes": [node.name for node in nodes],
        "selected_scene_id": scene_id,
        "selected_cluster_key": cluster_key,
        "nodes": by_node,
        "comparison": comparison,
    }


def target_nodes(config: ReplConfig, *, use_cluster: bool) -> list[Any]:
    if use_cluster:
        return list(config.nodes.values())
    return [config.nodes[config.publisher]]


def run_parallel(nodes: Sequence[Any], task: Any, *, worker_count: int) -> dict[str, dict[str, Any]]:
    results: dict[str, dict[str, Any]] = {}
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        future_by_node = {executor.submit(task, node): node for node in nodes}
        for future in as_completed(future_by_node):
            node = future_by_node[future]
            try:
                results[node.name] = future.result()
            except Exception as exc:
                results[node.name] = {
                    "node": node.name,
                    "role": node.role,
                    "server": node.server,
                    "ok": False,
                    "error": friendly_error(exc),
                }
    return results


def collect_node_anchor_report(
    config: ReplConfig,
    node_name: str,
    *,
    top_clusters: int,
    timeout: int,
) -> dict[str, Any]:
    node = config.nodes[node_name]
    connection = connect(node, config, database=config.database, timeout=timeout)
    try:
        return {
            "node": node.name,
            "role": node.role,
            "server": node.server,
            "ok": True,
            "identity": one_dict(
                connection,
                """
                SELECT
                    @@SERVERNAME AS sql_name,
                    DB_NAME() AS database_name,
                    CAST(SERVERPROPERTY('MachineName') AS nvarchar(256)) AS machine_name,
                    CAST(SERVERPROPERTY('Edition') AS nvarchar(256)) AS edition,
                    CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(256)) AS product_version
                """,
            ),
            "table_counts": table_counts(connection, config.schema, CORE_TABLES),
            "top_clusters": top_clusters_report(connection, config.schema, top_clusters),
        }
    finally:
        close_connection(connection)


def collect_node_bundle_report(
    config: ReplConfig,
    node_name: str,
    *,
    scene_id: str | None,
    cluster_key: str | None,
    timeout: int,
) -> dict[str, Any]:
    node = config.nodes[node_name]
    connection = connect(node, config, database=config.database, timeout=timeout)
    try:
        return {
            "node": node.name,
            "role": node.role,
            "server": node.server,
            "ok": True,
            "bundle": scene_bundle(connection, config.schema, scene_id=scene_id, cluster_key=cluster_key),
        }
    finally:
        close_connection(connection)


def select_scene_and_cluster(node_report: dict[str, Any] | None, requested_scene_id: str | None) -> tuple[str | None, str | None]:
    if requested_scene_id:
        cluster_key = None
        if node_report and node_report.get("ok"):
            for cluster in node_report.get("top_clusters", []):
                if cluster.get("canonical_scene_id") == requested_scene_id:
                    cluster_key = cluster.get("cluster_key")
                    break
        return requested_scene_id, cluster_key
    if not node_report or not node_report.get("ok"):
        return None, None
    clusters = node_report.get("top_clusters", [])
    if not clusters:
        return None, None
    strongest = clusters[0]
    return strongest.get("canonical_scene_id"), strongest.get("cluster_key")


def table_counts(connection: Any, schema: str, tables: Iterable[str]) -> list[dict[str, Any]]:
    names = list(tables)
    rows = all_dicts(
        connection,
        f"""
        SELECT
            expected.name AS table_name,
            CASE WHEN t.object_id IS NULL THEN CAST(0 AS bit) ELSE CAST(1 AS bit) END AS present,
            COALESCE(SUM(CASE WHEN p.index_id IN (0, 1) THEN p.row_count ELSE 0 END), 0) AS row_count
        FROM (VALUES {", ".join("(?)" for _ in names)}) AS expected(name)
        LEFT JOIN sys.schemas AS s ON s.name = ?
        LEFT JOIN sys.tables AS t ON t.name = expected.name AND t.schema_id = s.schema_id
        LEFT JOIN sys.dm_db_partition_stats AS p ON p.object_id = t.object_id
        GROUP BY expected.name, t.object_id
        ORDER BY expected.name
        """,
        *names,
        schema,
    )
    return [
        {
            "table_name": row["table_name"],
            "present": bool(row["present"]),
            "row_count": int(row["row_count"]),
        }
        for row in rows
    ]


def top_clusters_report(connection: Any, schema: str, limit: int) -> list[dict[str, Any]]:
    return all_dicts(
        connection,
        f"""
        SELECT TOP ({limit})
            cluster_key,
            canonical_scene_id,
            cluster_type,
            region,
            label,
            summary,
            primary_node_key,
            confidence,
            member_count
        FROM {qualified_name(schema, "tmp_graph_clusters")}
        ORDER BY confidence DESC, member_count DESC, cluster_key
        """,
    )


def scene_bundle(connection: Any, schema: str, *, scene_id: str | None, cluster_key: str | None) -> dict[str, Any]:
    if not scene_id:
        return empty_bundle("no scene id selected")
    anchor_cluster = find_anchor_cluster(connection, schema, scene_id=scene_id, cluster_key=cluster_key)
    selected_cluster_key = cluster_key or (anchor_cluster or {}).get("cluster_key")
    node_keys = top_node_keys(connection, schema, scene_id)
    scene = one_or_none(
        connection,
        f"""
        SELECT TOP (1)
            scene_key,
            canonical_scene_id,
            source_path,
            width,
            height,
            brightness,
            contrast,
            palette_json,
            mean_rgb_json,
            vector_model,
            vector_dim,
            source_sha256,
            content_hash
        FROM {qualified_name(schema, "tmp_scenes")}
        WHERE canonical_scene_id = ? OR scene_key = ?
        ORDER BY scene_key
        """,
        scene_id,
        scene_id,
    )
    return {
        "found": scene is not None or anchor_cluster is not None,
        "reason": None if scene is not None or anchor_cluster is not None else f"scene not found: {scene_id}",
        "scene": scene,
        "description": one_or_none(
            connection,
            f"""
            SELECT TOP (1)
                description_key,
                canonical_scene_id,
                title,
                summary,
                token_count,
                vector_model,
                vector_dim,
                source_path,
                content_hash
            FROM {qualified_name(schema, "tmp_descriptions")}
            WHERE canonical_scene_id = ?
            ORDER BY description_key
            """,
            scene_id,
        ),
        "anchor_cluster": anchor_cluster,
        "objects": all_dicts(
            connection,
            f"""
            SELECT TOP ({BUNDLE_LIMITS["objects"]})
                object_key,
                scene_key,
                source_path,
                crop_role,
                bbox_x,
                bbox_y,
                bbox_width,
                bbox_height,
                area_ratio,
                saliency_score,
                palette_json,
                mean_rgb_json,
                brightness,
                contrast
            FROM {qualified_name(schema, "tmp_objects")}
            WHERE canonical_scene_id = ?
            ORDER BY saliency_score DESC, area_ratio DESC, object_key
            """,
            scene_id,
        ),
        "cameras": all_dicts(
            connection,
            f"""
            SELECT TOP ({BUNDLE_LIMITS["cameras"]})
                camera_key,
                source_path,
                camera_name,
                location_text,
                look_at_text,
                angle_text,
                camera_height,
                camera_distance,
                camera_yaw,
                camera_pitch,
                camera_angle,
                snippet
            FROM {qualified_name(schema, "tmp_cameras")}
            WHERE canonical_scene_id = ?
            ORDER BY has_numeric_camera DESC, camera_key
            """,
            scene_id,
        ),
        "lights": all_dicts(
            connection,
            f"""
            SELECT TOP ({BUNDLE_LIMITS["lights"]})
                light_key,
                source_path,
                light_name,
                light_type,
                light_role,
                color_family,
                light_x,
                light_y,
                light_z,
                color_intensity,
                has_area_light,
                has_spotlight,
                has_shadowless,
                snippet
            FROM {qualified_name(schema, "tmp_lights")}
            WHERE canonical_scene_id = ?
            ORDER BY light_role, light_key
            """,
            scene_id,
        ),
        "materials": all_dicts(
            connection,
            f"""
            SELECT TOP ({BUNDLE_LIMITS["materials"]})
                material_key,
                source_path,
                material_name,
                block_type,
                material_family,
                color_count,
                has_texture,
                has_pigment,
                has_finish,
                has_reflection,
                has_transparency,
                snippet
            FROM {qualified_name(schema, "tmp_materials")}
            WHERE canonical_scene_id = ?
            ORDER BY material_family, material_key
            """,
            scene_id,
        ),
        "spatial_nodes": all_dicts(
            connection,
            f"""
            SELECT TOP ({BUNDLE_LIMITS["spatial_nodes"]})
                node_key,
                node_type,
                label,
                source_table,
                source_key,
                space_kind,
                x_min,
                x_max,
                y_min,
                y_max,
                z_min,
                z_max,
                confidence
            FROM {qualified_name(schema, "tmp_spatial_nodes")}
            WHERE canonical_scene_id = ?
            ORDER BY confidence DESC, node_type, label
            """,
            scene_id,
        ),
        "spatial_edges": spatial_edges(connection, schema, node_keys),
        "cluster_members": cluster_members(connection, schema, selected_cluster_key),
    }


def empty_bundle(reason: str) -> dict[str, Any]:
    return {
        "found": False,
        "reason": reason,
        "scene": None,
        "description": None,
        "anchor_cluster": None,
        "objects": [],
        "cameras": [],
        "lights": [],
        "materials": [],
        "spatial_nodes": [],
        "spatial_edges": [],
        "cluster_members": [],
    }


def find_anchor_cluster(connection: Any, schema: str, *, scene_id: str, cluster_key: str | None) -> dict[str, Any] | None:
    if cluster_key:
        found = one_or_none(
            connection,
            f"""
            SELECT TOP (1)
                cluster_key,
                canonical_scene_id,
                cluster_type,
                region,
                label,
                summary,
                primary_node_key,
                confidence,
                member_count,
                retrieval_text
            FROM {qualified_name(schema, "tmp_graph_clusters")}
            WHERE cluster_key = ?
            """,
            cluster_key,
        )
        if found:
            return found
    return one_or_none(
        connection,
        f"""
        SELECT TOP (1)
            cluster_key,
            canonical_scene_id,
            cluster_type,
            region,
            label,
            summary,
            primary_node_key,
            confidence,
            member_count,
            retrieval_text
        FROM {qualified_name(schema, "tmp_graph_clusters")}
        WHERE canonical_scene_id = ?
        ORDER BY confidence DESC, member_count DESC, cluster_key
        """,
        scene_id,
    )


def top_node_keys(connection: Any, schema: str, scene_id: str) -> list[str]:
    rows = all_dicts(
        connection,
        f"""
        SELECT TOP (20) node_key
        FROM {qualified_name(schema, "tmp_spatial_nodes")}
        WHERE canonical_scene_id = ?
        ORDER BY confidence DESC, node_type, label
        """,
        scene_id,
    )
    return [str(row["node_key"]) for row in rows]


def spatial_edges(connection: Any, schema: str, node_keys: Sequence[str]) -> list[dict[str, Any]]:
    if not node_keys:
        return []
    placeholders = ", ".join("?" for _ in node_keys)
    return all_dicts(
        connection,
        f"""
        SELECT TOP ({BUNDLE_LIMITS["spatial_edges"]})
            from_node_key,
            edge_type,
            to_node_key,
            weight,
            distance,
            metadata_json
        FROM {qualified_name(schema, "tmp_spatial_edges")}
        WHERE from_node_key IN ({placeholders})
           OR to_node_key IN ({placeholders})
        ORDER BY weight DESC, from_node_key, to_node_key
        """,
        *node_keys,
        *node_keys,
    )


def cluster_members(connection: Any, schema: str, cluster_key: str | None) -> list[dict[str, Any]]:
    if not cluster_key:
        return []
    return all_dicts(
        connection,
        f"""
        SELECT TOP ({BUNDLE_LIMITS["cluster_members"]})
            cluster_key,
            node_key,
            member_role,
            weight,
            reason
        FROM {qualified_name(schema, "tmp_graph_cluster_members")}
        WHERE cluster_key = ?
        ORDER BY weight DESC, member_role, node_key
        """,
        cluster_key,
    )


def compare_nodes(
    config: ReplConfig,
    nodes: Sequence[dict[str, Any]],
    *,
    selected_scene_id: str | None,
    selected_cluster_key: str | None,
) -> dict[str, Any]:
    by_name = {node["node"]: node for node in nodes}
    publisher = by_name.get(config.publisher)
    failures: list[dict[str, Any]] = []
    row_count_mismatches: list[dict[str, Any]] = []
    bundle_mismatches: list[dict[str, Any]] = []

    for node in nodes:
        if not node.get("ok"):
            failures.append({"node": node.get("node"), "error": node.get("error", "query failed")})

    if publisher and publisher.get("ok"):
        publisher_counts = counts_by_table(publisher)
        publisher_signature = bundle_signature(publisher)
        for node in nodes:
            if node["node"] == config.publisher or not node.get("ok"):
                continue
            node_counts = counts_by_table(node)
            for table_name, expected_count in publisher_counts.items():
                actual_count = node_counts.get(table_name)
                if actual_count != expected_count:
                    row_count_mismatches.append(
                        {
                            "node": node["node"],
                            "table_name": table_name,
                            "publisher_count": expected_count,
                            "node_count": actual_count,
                        }
                    )
            signature = bundle_signature(node)
            if signature != publisher_signature:
                bundle_mismatches.append(
                    {
                        "node": node["node"],
                        "publisher_signature": publisher_signature,
                        "node_signature": signature,
                    }
                )
    else:
        failures.append({"node": config.publisher, "error": "publisher report unavailable"})

    return {
        "ok": not failures and not row_count_mismatches and not bundle_mismatches,
        "publisher": config.publisher,
        "selected_scene_id": selected_scene_id,
        "selected_cluster_key": selected_cluster_key,
        "failures": failures,
        "row_count_mismatches": row_count_mismatches,
        "bundle_mismatches": bundle_mismatches,
    }


def counts_by_table(node_report: dict[str, Any]) -> dict[str, int]:
    return {str(row["table_name"]): int(row["row_count"]) for row in node_report.get("table_counts", [])}


def bundle_signature(node_report: dict[str, Any]) -> dict[str, Any]:
    bundle = node_report.get("bundle") or {}
    scene = bundle.get("scene") or {}
    cluster = bundle.get("anchor_cluster") or {}
    return {
        "scene_key": scene.get("scene_key"),
        "canonical_scene_id": scene.get("canonical_scene_id"),
        "cluster_key": cluster.get("cluster_key"),
        "objects": len(bundle.get("objects") or []),
        "cameras": len(bundle.get("cameras") or []),
        "lights": len(bundle.get("lights") or []),
        "materials": len(bundle.get("materials") or []),
        "spatial_nodes": len(bundle.get("spatial_nodes") or []),
        "spatial_edges": len(bundle.get("spatial_edges") or []),
        "cluster_members": len(bundle.get("cluster_members") or []),
    }


def one_dict(connection: Any, sql: str, *params: Any) -> dict[str, Any]:
    row = one_or_none(connection, sql, *params)
    if row is None:
        raise ClusterQueryError("query returned no rows")
    return row


def one_or_none(connection: Any, sql: str, *params: Any) -> dict[str, Any] | None:
    rows = all_dicts(connection, sql, *params)
    return rows[0] if rows else None


def all_dicts(connection: Any, sql: str, *params: Any) -> list[dict[str, Any]]:
    cursor = connection.cursor()
    try:
        cursor.execute(sql, *params)
        names = [str(column[0]) for column in cursor.description]
        return [normalize_row(dict(zip(names, row))) for row in cursor.fetchall()]
    finally:
        cursor.close()


def normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    return {key: json_safe(value) for key, value in row.items()}


def json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, bytes):
        return {"bytes": len(value)}
    return value


def qualified_name(schema: str, table: str) -> str:
    return f"{quote_identifier(schema)}.{quote_identifier(table)}"


def quote_identifier(value: str) -> str:
    return "[" + value.replace("]", "]]") + "]"


def close_connection(connection: Any) -> None:
    try:
        connection.close()
    except Exception:
        pass


def format_report(report: dict[str, Any], output_format: str) -> str:
    if output_format == "json":
        return json.dumps(report, indent=2, sort_keys=True)
    return format_text_report(report)


def format_text_report(report: dict[str, Any]) -> str:
    lines = [
        f"WGD scene cluster query for {report['database']}.{report['schema']}",
        f"Use cluster: {report.get('use_cluster')}",
        f"Worker count: {report.get('worker_count')}",
        f"Queried nodes: {', '.join(report.get('queried_nodes') or [])}",
        f"Selected scene: {report.get('selected_scene_id') or '<none>'}",
        f"Selected cluster: {report.get('selected_cluster_key') or '<none>'}",
        "",
    ]
    for node in report["nodes"]:
        lines.extend(format_node(node))
        lines.append("")
    lines.extend(format_comparison(report["comparison"]))
    return "\n".join(lines)


def format_node(node: dict[str, Any]) -> list[str]:
    lines = [f"[{node['node']}] {node.get('role')} {node.get('server')}"]
    if not node.get("ok"):
        lines.append(f"  ERROR: {node.get('error', 'query failed')}")
        return lines
    identity = node.get("identity") or {}
    lines.append(
        "  identity: "
        f"{identity.get('sql_name')} / {identity.get('database_name')} / "
        f"{identity.get('edition')} / {identity.get('product_version')}"
    )
    lines.append("  core table counts:")
    for row in node.get("table_counts", []):
        state = "present" if row.get("present") else "missing"
        lines.append(f"    {row['table_name']}: {row['row_count']} ({state})")
    lines.append("  top clusters:")
    for cluster in node.get("top_clusters", []):
        lines.append(
            "    "
            f"{cluster.get('cluster_key')} scene={cluster.get('canonical_scene_id')} "
            f"type={cluster.get('cluster_type')} region={cluster.get('region')} "
            f"confidence={cluster.get('confidence')} members={cluster.get('member_count')} "
            f"label={cluster.get('label')}"
        )
    bundle = node.get("bundle") or {}
    if bundle:
        lines.append("  construction bundle:")
        scene = bundle.get("scene") or {}
        description = bundle.get("description") or {}
        cluster = bundle.get("anchor_cluster") or {}
        lines.append(f"    scene: {scene.get('scene_key')} {scene.get('source_path')}")
        if description:
            lines.append(f"    description: {description.get('title') or '<untitled>'}")
        if cluster:
            lines.append(f"    anchor cluster: {cluster.get('cluster_key')} {cluster.get('label')}")
        if bundle.get("reason"):
            lines.append(f"    note: {bundle.get('reason')}")
        lines.append(
            "    counts: "
            f"objects={len(bundle.get('objects') or [])}, "
            f"cameras={len(bundle.get('cameras') or [])}, "
            f"lights={len(bundle.get('lights') or [])}, "
            f"materials={len(bundle.get('materials') or [])}, "
            f"spatial_nodes={len(bundle.get('spatial_nodes') or [])}, "
            f"spatial_edges={len(bundle.get('spatial_edges') or [])}, "
            f"cluster_members={len(bundle.get('cluster_members') or [])}"
        )
    return lines


def format_comparison(comparison: dict[str, Any]) -> list[str]:
    lines = ["Comparison:"]
    lines.append(f"  publisher: {comparison.get('publisher')}")
    lines.append(f"  status: {'OK' if comparison.get('ok') else 'MISMATCH/ERROR'}")
    if comparison.get("failures"):
        lines.append("  failures:")
        for failure in comparison["failures"]:
            lines.append(f"    {failure.get('node')}: {failure.get('error')}")
    if comparison.get("row_count_mismatches"):
        lines.append("  row-count mismatches:")
        for mismatch in comparison["row_count_mismatches"]:
            lines.append(
                "    "
                f"{mismatch['node']} {mismatch['table_name']}: "
                f"publisher={mismatch['publisher_count']} node={mismatch['node_count']}"
            )
    if comparison.get("bundle_mismatches"):
        lines.append("  bundle mismatches:")
        for mismatch in comparison["bundle_mismatches"]:
            lines.append(f"    {mismatch['node']}: construction bundle differs from publisher")
    return lines


def friendly_error(exc: BaseException) -> str:
    text = str(exc).strip()
    return text or exc.__class__.__name__


if __name__ == "__main__":
    raise SystemExit(main())
