#!/usr/bin/env python3
"""Incrementally sync local WGD SQLite tables into the SQL Server WGD cluster."""

from __future__ import annotations

import argparse
import base64
from collections import Counter, defaultdict
import csv
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
import math
import os
from pathlib import Path
import shlex
import sqlite3
import subprocess
import sys
import time
from typing import Any, Callable, Iterable, Iterator, Mapping, Sequence


ROOT = Path(__file__).resolve().parents[2]
REPLICATION_DIR = ROOT / "wgd" / "replication"
if str(REPLICATION_DIR) not in sys.path:
    sys.path.insert(0, str(REPLICATION_DIR))

from wgd_replication import (  # noqa: E402
    DEFAULT_CONFIG,
    ReplConfig,
    ReplError,
    agent_endpoint_for,
    connect,
    load_repl_config,
    read_password,
)


DEFAULT_DB_DIR = ROOT / "vector_databases"
DEFAULT_STATUS_DB = Path(__file__).with_name("wgd_sync_status.sqlite")
DEFAULT_BATCH_SIZE = 1000
PUBLISHER_NODE = "NODE010"
SYNC_TARGET_NODE = PUBLISHER_NODE
SQL_MERGE_ACTION_COLUMN = "merge_action"
SQL_SURROGATE_OUTPUT_COLUMN = "sql_surrogate_id"
STATUS_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%f%z"
SCAN_PROGRESS_INTERVAL_ROWS = 50000
ProgressCallback = Callable[[Mapping[str, Any]], None]


class SyncError(RuntimeError):
    """Raised when sync cannot continue."""


class HelpFormatter(argparse.ArgumentDefaultsHelpFormatter, argparse.RawDescriptionHelpFormatter):
    """Show defaults while preserving command examples."""


@dataclass(frozen=True)
class TableSpec:
    name: str
    source_db: str
    source_table: str
    target_table: str
    key_columns: tuple[str, ...]
    surrogate_column: str | None = None

    @property
    def target_short_name(self) -> str:
        return self.target_table.removeprefix("tmp_")


@dataclass(frozen=True)
class SourceRow:
    values: dict[str, Any]
    logical_key: tuple[Any, ...]
    logical_key_json: str
    row_hash: str


@dataclass(frozen=True)
class TableScan:
    spec: TableSpec
    columns: tuple[str, ...]
    rows_scanned: int
    rows_skipped: int
    changed_rows: tuple[SourceRow, ...]
    started_at: str
    finished_at: str

    @property
    def rows_staged(self) -> int:
        return len(self.changed_rows)


@dataclass(frozen=True)
class MergeResult:
    inserted: int = 0
    updated: int = 0
    failed: int = 0
    surrogate_ids_by_key: Mapping[str, int | None] | None = None


@dataclass(frozen=True)
class PurgeCandidate:
    table_name: str
    logical_key_json: str


@dataclass(frozen=True)
class PurgeResult:
    table_name: str
    candidates: int = 0
    purged: int = 0
    error: str = ""


@dataclass(frozen=True)
class VerificationRow:
    table_name: str
    logical_key_json: str
    row_hash: str


@dataclass(frozen=True)
class VerificationResult:
    node: str
    table_name: str
    row_count: int | None
    changed_row_verification_count: int
    verification_state: str
    lag_seconds: float
    error: str = ""


@dataclass(frozen=True)
class StagedTable:
    spec: TableSpec
    columns: tuple[str, ...]
    sqlite_types: Mapping[str, str]
    blob_columns: tuple[str, ...]
    staging_table: str
    csv_path: Path
    rows_scanned: int
    rows_skipped: int
    rows_staged: int
    purge_candidates: tuple[PurgeCandidate, ...] = ()


SOURCE_DB_FILES = {
    "scene_vectors": "scene_vectors.sqlite",
    "object_vectors": "object_vectors.sqlite",
    "pov_vectors": "pov_vectors.sqlite",
    "material_vectors": "material_vectors.sqlite",
    "camera_vectors": "camera_vectors.sqlite",
    "light_vectors": "light_vectors.sqlite",
    "description_vectors": "description_vectors.sqlite",
    "space_graph": "space_graph.sqlite",
    "meta_source_graph": "meta_source_graph.sqlite",
}


TABLE_SPECS: tuple[TableSpec, ...] = (
    TableSpec("metadata", "space_graph", "metadata", "tmp_metadata", ("key",)),
    TableSpec("scenes", "scene_vectors", "scenes", "tmp_scenes", ("scene_key",), "scene_id"),
    TableSpec("objects", "object_vectors", "objects", "tmp_objects", ("object_key",), "object_id"),
    TableSpec("povs", "pov_vectors", "povs", "tmp_povs", ("pov_key",), "pov_id"),
    TableSpec("materials", "material_vectors", "materials", "tmp_materials", ("material_key",), "material_id"),
    TableSpec("cameras", "camera_vectors", "cameras", "tmp_cameras", ("camera_key",), "camera_id"),
    TableSpec("lights", "light_vectors", "lights", "tmp_lights", ("light_key",), "light_id"),
    TableSpec(
        "descriptions",
        "description_vectors",
        "descriptions",
        "tmp_descriptions",
        ("description_key",),
        "description_id",
    ),
    TableSpec("spatial_nodes", "space_graph", "spatial_nodes", "tmp_spatial_nodes", ("node_key",), "node_id"),
    TableSpec(
        "spatial_edges",
        "space_graph",
        "spatial_edges",
        "tmp_spatial_edges",
        ("from_node_key", "to_node_key", "edge_type"),
        "edge_id",
    ),
    TableSpec("spatial_cells", "space_graph", "spatial_cells", "tmp_spatial_cells", ("cell_key",), "cell_id"),
    TableSpec("cell_vectors", "space_graph", "cell_vectors", "tmp_cell_vectors", ("cell_key", "modality")),
    TableSpec("graph_clusters", "space_graph", "graph_clusters", "tmp_graph_clusters", ("cluster_key",), "cluster_id"),
    TableSpec(
        "graph_cluster_members",
        "space_graph",
        "graph_cluster_members",
        "tmp_graph_cluster_members",
        ("cluster_key", "node_key", "member_role"),
    ),
    TableSpec("cluster_vectors", "space_graph", "cluster_vectors", "tmp_cluster_vectors", ("cluster_key", "modality")),
    TableSpec(
        "source_files",
        "meta_source_graph",
        "source_files",
        "tmp_source_files",
        ("file_key",),
        "file_id",
    ),
    TableSpec(
        "source_symbols",
        "meta_source_graph",
        "source_symbols",
        "tmp_source_symbols",
        ("symbol_key",),
        "symbol_id",
    ),
    TableSpec(
        "source_chunks",
        "meta_source_graph",
        "source_chunks",
        "tmp_source_chunks",
        ("chunk_key",),
        "chunk_id",
    ),
    TableSpec(
        "source_edges",
        "meta_source_graph",
        "source_edges",
        "tmp_source_edges",
        ("from_key", "to_key", "edge_type"),
        "edge_id",
    ),
)


PURGE_TABLE_ORDER = (
    "source_edges",
    "source_chunks",
    "source_symbols",
    "source_files",
    "graph_cluster_members",
    "cluster_vectors",
    "graph_clusters",
    "cell_vectors",
    "spatial_edges",
    "spatial_nodes",
    "spatial_cells",
    "objects",
    "materials",
    "cameras",
    "lights",
    "povs",
    "descriptions",
    "scenes",
    "metadata",
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Incrementally sync WGD SQLite graph/vector tables to SQL Server.",
        formatter_class=HelpFormatter,
        epilog=(
            "Examples:\n"
            "  python3 wgd/sync/wgd_sqlite_sync.py dry-run\n"
            "  python3 wgd/sync/wgd_sqlite_sync.py sync --table scenes --table spatial_nodes\n"
            "  python3 wgd/sync/wgd_sqlite_sync.py generate-sql --output-dir /tmp/wgd-sync-package\n"
            "  python3 wgd/sync/wgd_sqlite_sync.py rollback-test --output-root /tmp/wgd-sync-rollback-test\n"
            "  python3 wgd/sync/wgd_sqlite_sync.py verify --timeout-seconds 300\n"
            "  python3 wgd/sync/wgd_sqlite_sync.py purge --confirm-purge\n"
            "  python3 wgd/sync/wgd_sqlite_sync.py status"
        ),
    )
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG, help="WGD replication nodes config.")
    parser.add_argument("--db-dir", type=Path, default=DEFAULT_DB_DIR, help="Directory containing WGD SQLite DB files.")
    parser.add_argument("--status-db", type=Path, default=DEFAULT_STATUS_DB, help="Sidecar SQLite sync-status DB.")
    parser.add_argument("--schema", default=None, help="Target SQL Server schema. Defaults to the replication config schema.")
    parser.add_argument("--table", action="append", help="Limit to a logical table name or tmp_* table; may be repeated.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    generate = subparsers.add_parser("generate-sql", help="write an incremental SQL+CSV staging package")
    add_late_common_options(generate)
    generate.add_argument("--output-dir", type=Path, required=True, help="Directory to write the SQL package into.")
    generate.add_argument("--include-purge", action="store_true", help="Also emit opt-in purge DELETE blocks.")
    generate.add_argument(
        "--transaction-scope",
        choices=("per-table", "whole-package", "per-batch"),
        default="per-table",
        help="How generated SQL transaction blocks are grouped.",
    )
    generate.add_argument(
        "--rollback-mode",
        choices=("variable", "always-rollback", "commit"),
        default="variable",
        help="Whether generated SQL rolls back for validation or commits.",
    )
    generate.add_argument("--package-id", help="Stable package id for generated staging table names.")
    generate.add_argument("--format", choices=("text", "json"), default="text", help="Output format.")

    rollback_test_parser = subparsers.add_parser(
        "rollback-test",
        help="generate and optionally execute rollback validation packages",
    )
    add_late_common_options(rollback_test_parser)
    rollback_test_parser.add_argument(
        "--output-root",
        type=Path,
        default=Path("/tmp/wgd-sync-rollback-test"),
        help="Root directory for metadata/vector/full rollback packages.",
    )
    rollback_test_parser.add_argument(
        "--vector-table",
        default="cluster_vectors",
        help="Vector table used for the focused vector conversion phase.",
    )
    rollback_test_parser.add_argument(
        "--skip-full",
        action="store_true",
        help="Skip the final full/selected package rollback phase.",
    )
    rollback_test_parser.add_argument("--include-purge", action="store_true", help="Also generate an explicit purge rollback package.")
    rollback_test_parser.add_argument(
        "--execute",
        action="store_true",
        help="Load staging and execute sync.sql in rollback mode against NODE010.",
    )
    rollback_test_parser.add_argument(
        "--transaction-scope",
        choices=("per-table", "whole-package", "per-batch"),
        default="per-table",
        help="Transaction scope for generated rollback SQL.",
    )
    rollback_test_parser.add_argument("--package-prefix", help="Stable prefix for generated package ids.")
    rollback_test_parser.add_argument("--sqlcmd-bin", default="sqlcmd", help="sqlcmd executable used with --execute.")
    rollback_test_parser.add_argument(
        "--mssql-config",
        type=Path,
        help="Optional mssql_csv_loader config path passed to load_staging.sh as MSSQL_CONFIG.",
    )
    rollback_test_parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print live rollback package generation progress with percentages to stderr.",
    )
    rollback_test_parser.add_argument("--format", choices=("text", "json"), default="text", help="Output format.")

    sync = subparsers.add_parser("sync", help="upsert changed rows into NODE010")
    add_late_common_options(sync)
    sync.add_argument("--batch-size", type=positive_int, default=DEFAULT_BATCH_SIZE, help="Rows per SQL MERGE batch.")
    sync.add_argument("--purge-missing", action="store_true", help="Delete SQL rows absent from the source after MERGE.")
    sync.add_argument("--confirm-purge", action="store_true", help="Required with --purge-missing to physically delete rows.")
    sync.add_argument("--verify-subscribers", action="store_true", help="Run verification after sync completes.")
    sync.add_argument("--verify-timeout-seconds", type=nonnegative_float, default=0.0, help="Post-sync verify timeout.")
    sync.add_argument("--verify-poll-seconds", type=positive_float, default=10.0, help="Post-sync verify poll interval.")
    sync.add_argument("--format", choices=("text", "json"), default="text", help="Output format.")

    dry_run = subparsers.add_parser("dry-run", help="report rows that would be merged without SQL Server writes")
    add_late_common_options(dry_run)
    dry_run.add_argument("--include-purge", action="store_true", help="Also report rows that would be purge candidates.")
    dry_run.add_argument("--format", choices=("text", "json"), default="text", help="Output format.")

    verify = subparsers.add_parser("verify", help="verify publisher/subscriber row counts and changed row hashes")
    add_late_common_options(verify)
    verify.add_argument("--run-id", type=positive_int, help="Sync run id to verify. Defaults to latest successful sync run.")
    verify.add_argument("--timeout-seconds", type=nonnegative_float, default=0.0, help="Retry lagging checks until timeout.")
    verify.add_argument("--poll-seconds", type=positive_float, default=10.0, help="Seconds between retry attempts.")
    verify.add_argument("--format", choices=("text", "json"), default="text", help="Output format.")

    status = subparsers.add_parser("status", help="print latest sync-status summary")
    add_late_common_options(status)
    status.add_argument("--format", choices=("text", "json"), default="text", help="Output format.")

    purge = subparsers.add_parser("purge", help="delete SQL rows missing from the latest successful source scan")
    add_late_common_options(purge)
    purge.add_argument("--run-id", type=positive_int, help="Successful sync run id to purge against. Defaults to latest.")
    purge.add_argument("--batch-size", type=positive_int, default=DEFAULT_BATCH_SIZE, help="Rows per SQL DELETE batch.")
    purge.add_argument("--confirm-purge", action="store_true", help="Physically delete purge candidates from NODE010.")
    purge.add_argument("--format", choices=("text", "json"), default="text", help="Output format.")
    return parser.parse_args(argv)


def add_late_common_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--config", type=Path, default=argparse.SUPPRESS, help=argparse.SUPPRESS)
    parser.add_argument("--db-dir", type=Path, default=argparse.SUPPRESS, help=argparse.SUPPRESS)
    parser.add_argument("--status-db", type=Path, default=argparse.SUPPRESS, help=argparse.SUPPRESS)
    parser.add_argument("--schema", default=argparse.SUPPRESS, help=argparse.SUPPRESS)
    parser.add_argument("--table", action="append", default=argparse.SUPPRESS, help=argparse.SUPPRESS)


def positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be an integer") from exc
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return parsed


def nonnegative_float(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a number") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be at least 0")
    return parsed


def positive_float(value: str) -> float:
    parsed = nonnegative_float(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than 0")
    return parsed


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        if args.command == "status":
            report = status_report(args.status_db)
            print(format_status_report(report, args.format))
            return 0

        config = load_repl_config(args.config)
        schema = args.schema or config.schema
        specs = selected_specs(args.table)

        if args.command == "generate-sql":
            report = generate_sql_package(
                db_dir=args.db_dir,
                status_db=args.status_db,
                specs=specs,
                schema=schema,
                output_dir=args.output_dir,
                include_purge=args.include_purge,
                transaction_scope=args.transaction_scope,
                rollback_mode=args.rollback_mode,
                package_id=args.package_id,
            )
            print(format_generate_sql_report(report, args.format))
            return 0

        if args.command == "rollback-test":
            report = rollback_test_phase(
                config=config,
                db_dir=args.db_dir,
                status_db=args.status_db,
                specs=specs,
                schema=schema,
                output_root=args.output_root,
                vector_table=args.vector_table,
                include_full=not args.skip_full,
                include_purge=args.include_purge,
                transaction_scope=args.transaction_scope,
                package_prefix=args.package_prefix,
                execute=args.execute,
                sqlcmd_bin=args.sqlcmd_bin,
                mssql_config=args.mssql_config,
                progress=stderr_progress_reporter if args.verbose else None,
            )
            print(format_rollback_test_report(report, args.format))
            return 0 if report["state"] in {"GENERATED", "SUCCEEDED"} else 2

        if args.command == "dry-run":
            report = dry_run(
                db_dir=args.db_dir,
                status_db=args.status_db,
                specs=specs,
                include_purge=args.include_purge,
            )
            print(format_scan_report(report, args.format, title="WGD dry-run"))
            return 0

        if args.command == "sync":
            if args.purge_missing and not args.confirm_purge:
                raise SyncError("--purge-missing requires --confirm-purge")
            report = sync(
                config=config,
                db_dir=args.db_dir,
                status_db=args.status_db,
                specs=specs,
                schema=schema,
                batch_size=args.batch_size,
                purge_missing=args.purge_missing,
            )
            if args.verify_subscribers and report["state"] == "SUCCEEDED":
                verify_report = verify(
                    config=config,
                    db_dir=args.db_dir,
                    status_db=args.status_db,
                    specs=specs,
                    schema=schema,
                    run_id=int(report["run_id"]),
                    timeout_seconds=args.verify_timeout_seconds,
                    poll_seconds=args.verify_poll_seconds,
                )
                report["verification"] = verify_report
            print(format_scan_report(report, args.format, title="WGD sync"))
            return 0 if report["state"] == "SUCCEEDED" else 2

        if args.command == "verify":
            report = verify(
                config=config,
                db_dir=args.db_dir,
                status_db=args.status_db,
                specs=specs,
                schema=schema,
                run_id=args.run_id,
                timeout_seconds=args.timeout_seconds,
                poll_seconds=args.poll_seconds,
            )
            print(format_verify_report(report, args.format))
            return 0 if report["state"] == "OK" else 2

        if args.command == "purge":
            report = purge(
                config=config,
                status_db=args.status_db,
                specs=specs,
                schema=schema,
                run_id=args.run_id,
                batch_size=args.batch_size,
                confirm_purge=args.confirm_purge,
            )
            print(format_purge_report(report, args.format))
            return 0 if report["state"] in {"DRY_RUN", "SUCCEEDED"} else 2

        raise SyncError(f"unknown command: {args.command}")
    except (ReplError, SyncError, sqlite3.Error) as exc:
        print(f"wgd-sqlite-sync: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:
        print(f"wgd-sqlite-sync: {exc}", file=sys.stderr)
        return 1


def selected_specs(table_names: Sequence[str] | None) -> tuple[TableSpec, ...]:
    if not table_names:
        return TABLE_SPECS
    by_name: dict[str, TableSpec] = {}
    for spec in TABLE_SPECS:
        for name in {spec.name, spec.source_table, spec.target_table, spec.target_short_name}:
            by_name[name.casefold()] = spec
    specs: list[TableSpec] = []
    unknown: list[str] = []
    for table_name in table_names:
        spec = by_name.get(table_name.casefold())
        if spec is None:
            unknown.append(table_name)
            continue
        if spec not in specs:
            specs.append(spec)
    if unknown:
        valid = ", ".join(spec.name for spec in TABLE_SPECS)
        raise SyncError(f"unknown table(s): {', '.join(unknown)}; valid tables: {valid}")
    return tuple(specs)


def source_db_path(db_dir: Path, source_db: str) -> Path:
    try:
        filename = SOURCE_DB_FILES[source_db]
    except KeyError as exc:
        raise SyncError(f"unknown source database key: {source_db}") from exc
    return Path(db_dir) / filename


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime(STATUS_TIME_FORMAT)


class StatusStore:
    def __init__(self, path: Path, *, create: bool) -> None:
        self.path = Path(path)
        if create:
            self.path.parent.mkdir(parents=True, exist_ok=True)
        if not create and not self.path.exists():
            self.connection = None
            return
        self.connection = sqlite3.connect(self.path)
        self.connection.row_factory = sqlite3.Row
        if create:
            self.initialize()

    def close(self) -> None:
        if self.connection is not None:
            self.connection.close()

    def __enter__(self) -> StatusStore:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.close()

    def require_connection(self) -> sqlite3.Connection:
        if self.connection is None:
            raise SyncError(f"sync status DB does not exist: {self.path}")
        return self.connection

    def initialize(self) -> None:
        con = self.require_connection()
        con.executescript(
            """
            CREATE TABLE IF NOT EXISTS sync_runs (
                run_id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_manifest_hash TEXT,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                mode TEXT NOT NULL,
                state TEXT NOT NULL,
                error TEXT
            );

            CREATE TABLE IF NOT EXISTS sync_table_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER NOT NULL,
                node TEXT NOT NULL,
                table_name TEXT NOT NULL,
                rows_scanned INTEGER NOT NULL DEFAULT 0,
                rows_skipped INTEGER NOT NULL DEFAULT 0,
                rows_staged INTEGER NOT NULL DEFAULT 0,
                rows_inserted INTEGER NOT NULL DEFAULT 0,
                rows_updated INTEGER NOT NULL DEFAULT 0,
                rows_failed INTEGER NOT NULL DEFAULT 0,
                rows_purged INTEGER NOT NULL DEFAULT 0,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                elapsed_seconds REAL NOT NULL DEFAULT 0,
                error TEXT,
                FOREIGN KEY (run_id) REFERENCES sync_runs(run_id)
            );

            CREATE TABLE IF NOT EXISTS sync_row_state (
                table_name TEXT NOT NULL,
                logical_key_json TEXT NOT NULL,
                row_hash TEXT NOT NULL,
                target_node TEXT NOT NULL,
                last_successful_run_id INTEGER,
                last_seen_run_id INTEGER,
                sql_surrogate_id INTEGER,
                purged_at TEXT,
                purged_run_id INTEGER,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (table_name, logical_key_json, target_node)
            );

            CREATE TABLE IF NOT EXISTS cluster_verify_state (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER NOT NULL,
                node TEXT NOT NULL,
                table_name TEXT NOT NULL,
                row_count INTEGER,
                changed_row_verification_count INTEGER NOT NULL DEFAULT 0,
                verification_state TEXT NOT NULL,
                lag_seconds REAL NOT NULL DEFAULT 0,
                error TEXT,
                checked_at TEXT NOT NULL,
                FOREIGN KEY (run_id) REFERENCES sync_runs(run_id)
            );

            CREATE INDEX IF NOT EXISTS idx_sync_row_state_seen
                ON sync_row_state(table_name, target_node, last_seen_run_id);
            CREATE INDEX IF NOT EXISTS idx_sync_row_state_success
                ON sync_row_state(last_successful_run_id);
            CREATE INDEX IF NOT EXISTS idx_cluster_verify_run
                ON cluster_verify_state(run_id, node, table_name);
            """
        )
        ensure_sqlite_column(con, "sync_table_runs", "rows_purged", "INTEGER NOT NULL DEFAULT 0")
        ensure_sqlite_column(con, "sync_row_state", "purged_at", "TEXT")
        ensure_sqlite_column(con, "sync_row_state", "purged_run_id", "INTEGER")
        con.commit()

    def create_run(self, *, mode: str, source_manifest_hash: str | None) -> int:
        con = self.require_connection()
        cur = con.execute(
            """
            INSERT INTO sync_runs (source_manifest_hash, started_at, mode, state)
            VALUES (?, ?, ?, 'RUNNING')
            """,
            (source_manifest_hash, utc_now(), mode),
        )
        con.commit()
        return int(cur.lastrowid)

    def finish_run(self, run_id: int, *, state: str, error: str | None = None) -> None:
        con = self.require_connection()
        con.execute(
            """
            UPDATE sync_runs
            SET finished_at = ?, state = ?, error = ?
            WHERE run_id = ?
            """,
            (utc_now(), state, error, run_id),
        )
        con.commit()

    def record_table_run(
        self,
        *,
        run_id: int,
        node: str,
        table_name: str,
        scan: TableScan,
        merge: MergeResult,
        rows_purged: int = 0,
        error: str | None = None,
    ) -> None:
        con = self.require_connection()
        started = parse_status_time(scan.started_at)
        finished = parse_status_time(scan.finished_at)
        elapsed = max(0.0, (finished - started).total_seconds())
        con.execute(
            """
            INSERT INTO sync_table_runs (
                run_id, node, table_name, rows_scanned, rows_skipped, rows_staged,
                rows_inserted, rows_updated, rows_failed, rows_purged, started_at, finished_at,
                elapsed_seconds, error
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                node,
                table_name,
                scan.rows_scanned,
                scan.rows_skipped,
                scan.rows_staged,
                merge.inserted,
                merge.updated,
                merge.failed,
                rows_purged,
                scan.started_at,
                scan.finished_at,
                elapsed,
                error,
            ),
        )
        con.commit()

    def successful_hashes(self, *, table_name: str, target_node: str) -> dict[str, str]:
        con = self.require_connection()
        rows = con.execute(
            """
            SELECT logical_key_json, row_hash
            FROM sync_row_state
            WHERE table_name = ?
              AND target_node = ?
              AND last_successful_run_id IS NOT NULL
            """,
            (table_name, target_node),
        ).fetchall()
        return {str(row["logical_key_json"]): str(row["row_hash"]) for row in rows}

    def mark_seen(self, *, table_name: str, keys: Iterable[str], target_node: str, run_id: int) -> None:
        key_list = list(keys)
        if not key_list:
            return
        con = self.require_connection()
        con.executemany(
            """
            UPDATE sync_row_state
            SET last_seen_run_id = ?, updated_at = ?
            WHERE table_name = ? AND logical_key_json = ? AND target_node = ?
            """,
            [(run_id, utc_now(), table_name, key, target_node) for key in key_list],
        )
        con.commit()

    def mark_success(
        self,
        *,
        table_name: str,
        rows: Iterable[SourceRow],
        target_node: str,
        run_id: int,
        surrogate_ids_by_key: Mapping[str, int | None] | None = None,
    ) -> None:
        row_list = list(rows)
        if not row_list:
            return
        surrogate_ids_by_key = surrogate_ids_by_key or {}
        con = self.require_connection()
        now = utc_now()
        con.executemany(
            """
            INSERT INTO sync_row_state (
                table_name, logical_key_json, row_hash, target_node,
                last_successful_run_id, last_seen_run_id, sql_surrogate_id, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(table_name, logical_key_json, target_node) DO UPDATE SET
                row_hash = excluded.row_hash,
                last_successful_run_id = excluded.last_successful_run_id,
                last_seen_run_id = excluded.last_seen_run_id,
                sql_surrogate_id = COALESCE(excluded.sql_surrogate_id, sync_row_state.sql_surrogate_id),
                purged_at = NULL,
                purged_run_id = NULL,
                updated_at = excluded.updated_at
            """,
            [
                (
                    table_name,
                    row.logical_key_json,
                    row.row_hash,
                    target_node,
                    run_id,
                    run_id,
                    surrogate_ids_by_key.get(row.logical_key_json),
                    now,
                )
                for row in row_list
            ],
        )
        con.commit()

    def completed_tables_for_run(self, run_id: int, *, target_node: str) -> set[str]:
        con = self.require_connection()
        rows = con.execute(
            """
            SELECT DISTINCT table_name
            FROM sync_table_runs
            WHERE run_id = ?
              AND node = ?
              AND finished_at IS NOT NULL
              AND error IS NULL
            """,
            (run_id, target_node),
        ).fetchall()
        return {str(row["table_name"]) for row in rows}

    def purge_candidates(
        self,
        *,
        run_id: int,
        specs: Sequence[TableSpec],
        target_node: str,
    ) -> dict[str, list[PurgeCandidate]]:
        completed_tables = self.completed_tables_for_run(run_id, target_node=target_node)
        con = self.require_connection()
        grouped: dict[str, list[PurgeCandidate]] = {}
        for spec in order_specs_for_purge(specs):
            if spec.name not in completed_tables:
                grouped[spec.name] = []
                continue
            rows = con.execute(
                """
                SELECT logical_key_json
                FROM sync_row_state
                WHERE table_name = ?
                  AND target_node = ?
                  AND last_successful_run_id IS NOT NULL
                  AND (last_seen_run_id IS NULL OR last_seen_run_id < ?)
                  AND purged_run_id IS NULL
                ORDER BY logical_key_json
                """,
                (spec.name, target_node, run_id),
            ).fetchall()
            grouped[spec.name] = [
                PurgeCandidate(table_name=spec.name, logical_key_json=str(row["logical_key_json"]))
                for row in rows
            ]
        return grouped

    def successful_key_jsons(self, *, table_name: str, target_node: str) -> list[str]:
        con = self.require_connection()
        rows = con.execute(
            """
            SELECT logical_key_json
            FROM sync_row_state
            WHERE table_name = ?
              AND target_node = ?
              AND last_successful_run_id IS NOT NULL
              AND purged_run_id IS NULL
            ORDER BY logical_key_json
            """,
            (table_name, target_node),
        ).fetchall()
        return [str(row["logical_key_json"]) for row in rows]

    def mark_purged(
        self,
        *,
        table_name: str,
        candidates: Iterable[PurgeCandidate],
        target_node: str,
        purge_run_id: int,
    ) -> None:
        candidate_list = list(candidates)
        if not candidate_list:
            return
        con = self.require_connection()
        now = utc_now()
        con.executemany(
            """
            UPDATE sync_row_state
            SET purged_at = ?,
                purged_run_id = ?,
                updated_at = ?
            WHERE table_name = ?
              AND logical_key_json = ?
              AND target_node = ?
            """,
            [
                (now, purge_run_id, now, table_name, candidate.logical_key_json, target_node)
                for candidate in candidate_list
            ],
        )
        con.commit()

    def record_purge_table_run(
        self,
        *,
        run_id: int,
        node: str,
        table_name: str,
        candidates: int,
        purged: int,
        started_at: str,
        finished_at: str,
        error: str | None = None,
    ) -> None:
        con = self.require_connection()
        elapsed = max(0.0, (parse_status_time(finished_at) - parse_status_time(started_at)).total_seconds())
        con.execute(
            """
            INSERT INTO sync_table_runs (
                run_id, node, table_name, rows_scanned, rows_skipped, rows_staged,
                rows_inserted, rows_updated, rows_failed, rows_purged, started_at,
                finished_at, elapsed_seconds, error
            )
            VALUES (?, ?, ?, 0, 0, ?, 0, 0, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                node,
                table_name,
                candidates,
                0 if error is None else candidates,
                purged,
                started_at,
                finished_at,
                elapsed,
                error,
            ),
        )
        con.commit()

    def add_table_purge_count(self, *, run_id: int, table_name: str, node: str, purged: int) -> None:
        if purged <= 0:
            return
        con = self.require_connection()
        con.execute(
            """
            UPDATE sync_table_runs
            SET rows_purged = rows_purged + ?
            WHERE run_id = ?
              AND table_name = ?
              AND node = ?
            """,
            (purged, run_id, table_name, node),
        )
        con.commit()

    def latest_successful_run_id(self) -> int | None:
        con = self.require_connection()
        row = con.execute(
            "SELECT MAX(run_id) AS run_id FROM sync_runs WHERE mode = 'sync' AND state = 'SUCCEEDED'"
        ).fetchone()
        if row is None or row["run_id"] is None:
            return None
        return int(row["run_id"])

    def changed_rows_for_run(self, run_id: int, *, target_node: str) -> dict[str, list[VerificationRow]]:
        con = self.require_connection()
        rows = con.execute(
            """
            SELECT table_name, logical_key_json, row_hash
            FROM sync_row_state
            WHERE last_successful_run_id = ? AND target_node = ?
            ORDER BY table_name, logical_key_json
            """,
            (run_id, target_node),
        ).fetchall()
        grouped: dict[str, list[VerificationRow]] = defaultdict(list)
        for row in rows:
            grouped[str(row["table_name"])].append(
                VerificationRow(
                    table_name=str(row["table_name"]),
                    logical_key_json=str(row["logical_key_json"]),
                    row_hash=str(row["row_hash"]),
                )
            )
        return grouped

    def record_verification(self, *, run_id: int, result: VerificationResult) -> None:
        con = self.require_connection()
        con.execute(
            """
            INSERT INTO cluster_verify_state (
                run_id, node, table_name, row_count, changed_row_verification_count,
                verification_state, lag_seconds, error, checked_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                result.node,
                result.table_name,
                result.row_count,
                result.changed_row_verification_count,
                result.verification_state,
                result.lag_seconds,
                result.error,
                utc_now(),
            ),
        )
        con.commit()

    def latest_report(self) -> dict[str, Any]:
        con = self.require_connection()
        run = con.execute(
            "SELECT * FROM sync_runs ORDER BY run_id DESC LIMIT 1"
        ).fetchone()
        if run is None:
            return {"exists": True, "latest_run": None, "tables": [], "verification": []}
        table_rows = con.execute(
            """
            SELECT table_name, node,
                   SUM(rows_scanned) AS rows_scanned,
                   SUM(rows_skipped) AS rows_skipped,
                   SUM(rows_staged) AS rows_staged,
                   SUM(rows_inserted) AS rows_inserted,
                   SUM(rows_updated) AS rows_updated,
                   SUM(rows_failed) AS rows_failed,
                   SUM(rows_purged) AS rows_purged
            FROM sync_table_runs
            WHERE run_id = ?
            GROUP BY table_name, node
            ORDER BY table_name, node
            """,
            (run["run_id"],),
        ).fetchall()
        verify_rows = con.execute(
            """
            SELECT node, table_name, row_count, changed_row_verification_count,
                   verification_state, lag_seconds, error, checked_at
            FROM cluster_verify_state
            WHERE run_id = ?
            ORDER BY node, table_name, id
            """,
            (run["run_id"],),
        ).fetchall()
        missing_by_table = con.execute(
            """
            SELECT table_name, COUNT(*) AS missing_rows
            FROM sync_row_state
            WHERE last_seen_run_id IS NOT NULL
              AND last_seen_run_id < ?
              AND purged_run_id IS NULL
            GROUP BY table_name
            ORDER BY table_name
            """,
            (run["run_id"],),
        ).fetchall()
        purged_by_table = con.execute(
            """
            SELECT table_name, COUNT(*) AS purged_rows
            FROM sync_row_state
            WHERE purged_run_id = ?
            GROUP BY table_name
            ORDER BY table_name
            """,
            (run["run_id"],),
        ).fetchall()
        return {
            "exists": True,
            "latest_run": dict(run),
            "tables": [dict(row) for row in table_rows],
            "verification": [dict(row) for row in verify_rows],
            "source_missing_rows": [dict(row) for row in missing_by_table],
            "purged_rows": [dict(row) for row in purged_by_table],
        }


def parse_status_time(value: str) -> datetime:
    return datetime.strptime(value, STATUS_TIME_FORMAT)


def ensure_sqlite_column(con: sqlite3.Connection, table_name: str, column_name: str, column_sql: str) -> None:
    columns = {str(row[1]) for row in con.execute(f"PRAGMA table_info({quote_sqlite_identifier(table_name)})")}
    if column_name not in columns:
        con.execute(f"ALTER TABLE {quote_sqlite_identifier(table_name)} ADD COLUMN {quote_sqlite_identifier(column_name)} {column_sql}")


def canonical_value(value: Any) -> Any:
    if value is None:
        return {"type": "null"}
    if isinstance(value, memoryview):
        value = value.tobytes()
    if isinstance(value, (bytes, bytearray)):
        return {"type": "blob", "base64": base64.b64encode(bytes(value)).decode("ascii")}
    if isinstance(value, bool):
        return {"type": "bool", "value": value}
    if isinstance(value, int):
        return {"type": "int", "value": value}
    if isinstance(value, float):
        if math.isnan(value):
            return {"type": "float", "value": "NaN"}
        if math.isinf(value):
            return {"type": "float", "value": "Infinity" if value > 0 else "-Infinity"}
        return {"type": "float", "value": format(value, ".17g")}
    if isinstance(value, Decimal):
        return {"type": "decimal", "value": format(value, "f")}
    if isinstance(value, str):
        parsed = normalized_json_text(value)
        if parsed is not None:
            return {"type": "json", "value": parsed}
        return {"type": "text", "value": value}
    return {"type": type(value).__name__, "value": str(value)}


def normalized_json_text(value: str) -> Any | None:
    text = value.strip()
    if not text or text[0] not in "[{\"0123456789-tfn":
        return None
    try:
        return json.loads(text)
    except (TypeError, ValueError):
        return None


def canonical_row_payload(row: Mapping[str, Any], columns: Sequence[str], surrogate_column: str | None) -> list[Any]:
    return [
        [column, canonical_value(row[column])]
        for column in columns
        if column != surrogate_column
    ]


def canonical_row_hash(row: Mapping[str, Any], columns: Sequence[str], surrogate_column: str | None = None) -> str:
    payload = canonical_row_payload(row, columns, surrogate_column)
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def logical_key_tuple(spec: TableSpec, row: Mapping[str, Any]) -> tuple[Any, ...]:
    return tuple(row[column] for column in spec.key_columns)


def logical_key_json(spec: TableSpec, row: Mapping[str, Any] | Sequence[Any]) -> str:
    if isinstance(row, Mapping):
        key_values = logical_key_tuple(spec, row)
    else:
        key_values = tuple(row)
    payload = [
        [column, canonical_value(value)]
        for column, value in zip(spec.key_columns, key_values)
    ]
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def logical_key_values_from_json(key_json: str) -> tuple[Any, ...]:
    pairs = json.loads(key_json)
    return tuple(raw_value_from_canonical(value) for _, value in pairs)


def raw_value_from_canonical(value: Mapping[str, Any]) -> Any:
    value_type = value.get("type")
    if value_type == "null":
        return None
    if value_type == "blob":
        return base64.b64decode(str(value["base64"]).encode("ascii"))
    if value_type in {"bool", "int", "text", "json"}:
        return value.get("value")
    if value_type in {"float", "decimal"}:
        return value.get("value")
    return value.get("value")


def sqlite_table_columns(db_path: Path, table: str) -> tuple[str, ...]:
    if not db_path.exists():
        raise SyncError(f"source SQLite DB does not exist: {db_path}")
    with sqlite3.connect(db_path) as con:
        rows = con.execute(f"PRAGMA table_info({quote_sqlite_identifier(table)})").fetchall()
    columns = tuple(str(row[1]) for row in rows)
    if not columns:
        raise SyncError(f"source table {table!r} not found in {db_path}")
    return columns


def sqlite_table_column_types(db_path: Path, table: str) -> dict[str, str]:
    if not db_path.exists():
        raise SyncError(f"source SQLite DB does not exist: {db_path}")
    with sqlite3.connect(db_path) as con:
        rows = con.execute(f"PRAGMA table_info({quote_sqlite_identifier(table)})").fetchall()
    if not rows:
        raise SyncError(f"source table {table!r} not found in {db_path}")
    return {str(row[1]): str(row[2] or "").upper() for row in rows}


def blob_columns_for(columns: Sequence[str], sqlite_types: Mapping[str, str], rows: Sequence[SourceRow] = ()) -> tuple[str, ...]:
    blob_columns = {
        column
        for column in columns
        if "BLOB" in sqlite_types.get(column, "").upper()
    }
    for row in rows:
        for column in columns:
            value = row.values.get(column)
            if isinstance(value, memoryview):
                value = value.tobytes()
            if isinstance(value, (bytes, bytearray)):
                blob_columns.add(column)
    return tuple(column for column in columns if column in blob_columns)


def iter_sqlite_rows(db_path: Path, table: str, key_columns: Sequence[str]) -> Iterator[dict[str, Any]]:
    if not db_path.exists():
        raise SyncError(f"source SQLite DB does not exist: {db_path}")
    order_by = ", ".join(quote_sqlite_identifier(column) for column in key_columns)
    sql = f"SELECT * FROM {quote_sqlite_identifier(table)} ORDER BY {order_by}"
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    try:
        for row in con.execute(sql):
            yield dict(row)
    finally:
        con.close()


def scan_table(
    spec: TableSpec,
    *,
    db_dir: Path,
    status: StatusStore | None,
    run_id: int | None,
    target_node: str,
    progress: Callable[[int], None] | None = None,
    progress_interval_rows: int = SCAN_PROGRESS_INTERVAL_ROWS,
) -> TableScan:
    started_at = utc_now()
    db_path = source_db_path(db_dir, spec.source_db)
    columns = sqlite_table_columns(db_path, spec.source_table)
    existing_hashes = status.successful_hashes(table_name=spec.name, target_node=target_node) if status else {}
    changed_rows: list[SourceRow] = []
    skipped_keys: list[str] = []
    rows_scanned = 0
    rows_skipped = 0
    for row in iter_sqlite_rows(db_path, spec.source_table, spec.key_columns):
        rows_scanned += 1
        if progress is not None and (rows_scanned == 1 or rows_scanned % progress_interval_rows == 0):
            progress(rows_scanned)
        key_json = logical_key_json(spec, row)
        row_hash = canonical_row_hash(row, columns, spec.surrogate_column)
        if existing_hashes.get(key_json) == row_hash:
            rows_skipped += 1
            skipped_keys.append(key_json)
            continue
        changed_rows.append(
            SourceRow(
                values=row,
                logical_key=logical_key_tuple(spec, row),
                logical_key_json=key_json,
                row_hash=row_hash,
            )
        )
    if progress is not None and (rows_scanned == 0 or rows_scanned % progress_interval_rows != 0):
        progress(rows_scanned)
    if status is not None and run_id is not None:
        status.mark_seen(table_name=spec.name, keys=skipped_keys, target_node=target_node, run_id=run_id)
    return TableScan(
        spec=spec,
        columns=columns,
        rows_scanned=rows_scanned,
        rows_skipped=rows_skipped,
        changed_rows=tuple(changed_rows),
        started_at=started_at,
        finished_at=utc_now(),
    )


def source_key_jsons(spec: TableSpec, *, db_dir: Path) -> set[str]:
    db_path = source_db_path(db_dir, spec.source_db)
    select_columns = ", ".join(quote_sqlite_identifier(column) for column in spec.key_columns)
    order_by = ", ".join(quote_sqlite_identifier(column) for column in spec.key_columns)
    sql = f"SELECT {select_columns} FROM {quote_sqlite_identifier(spec.source_table)} ORDER BY {order_by}"
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    try:
        return {logical_key_json(spec, dict(row)) for row in con.execute(sql)}
    finally:
        con.close()


def preview_purge_candidate_count(
    spec: TableSpec,
    *,
    db_dir: Path,
    status: StatusStore | None,
    target_node: str,
) -> int:
    if status is None:
        return 0
    current_keys = source_key_jsons(spec, db_dir=db_dir)
    previous_keys = status.successful_key_jsons(table_name=spec.name, target_node=target_node)
    return sum(1 for key in previous_keys if key not in current_keys)


def sqlite_table_row_count(db_path: Path, table: str) -> int:
    if not db_path.exists():
        raise SyncError(f"source SQLite DB does not exist: {db_path}")
    with sqlite3.connect(db_path) as con:
        row = con.execute(f"SELECT COUNT(*) FROM {quote_sqlite_identifier(table)}").fetchone()
    if row is None:
        raise SyncError(f"source table {table!r} count returned no rows in {db_path}")
    return int(row[0])


def table_scan_progress(progress: ProgressCallback | None, *, spec: TableSpec, rows_total: int | None) -> Callable[[int], None] | None:
    if progress is None:
        return None

    def report(rows_scanned: int) -> None:
        fraction = 0.75
        if rows_total and rows_total > 0:
            fraction = min(0.75, max(0.0, rows_scanned / rows_total) * 0.75)
        progress(
            {
                "event": "table_scan_progress",
                "table": spec.name,
                "rows_scanned": rows_scanned,
                "rows_total": rows_total,
                "table_fraction": fraction,
            }
        )

    return report


def generate_sql_package(
    *,
    db_dir: Path,
    status_db: Path,
    specs: Sequence[TableSpec],
    schema: str,
    output_dir: Path,
    include_purge: bool = False,
    transaction_scope: str = "per-table",
    rollback_mode: str = "variable",
    package_id: str | None = None,
    progress: ProgressCallback | None = None,
) -> dict[str, Any]:
    if transaction_scope not in {"per-table", "whole-package", "per-batch"}:
        raise SyncError(f"unsupported transaction scope: {transaction_scope}")
    if rollback_mode not in {"variable", "always-rollback", "commit"}:
        raise SyncError(f"unsupported rollback mode: {rollback_mode}")

    package_id = package_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    safe_package_id = safe_sql_token(package_id)
    output_dir = Path(output_dir)
    staging_dir = output_dir / "staging"
    staging_dir.mkdir(parents=True, exist_ok=True)

    status = StatusStore(status_db, create=False) if Path(status_db).exists() else None
    staged_tables: list[StagedTable] = []
    row_counts: dict[str, int] = {}
    if progress is not None:
        row_counts = {
            spec.name: sqlite_table_row_count(source_db_path(db_dir, spec.source_db), spec.source_table)
            for spec in specs
        }
        progress(
            {
                "event": "package_start",
                "output_dir": str(output_dir),
                "tables_total": len(specs),
                "rows_total": sum(row_counts.values()),
            }
        )
    try:
        for index, spec in enumerate(specs, start=1):
            rows_total = row_counts.get(spec.name)
            if progress is not None:
                progress(
                    {
                        "event": "table_start",
                        "table": spec.name,
                        "table_index": index,
                        "tables_total": len(specs),
                        "rows_total": rows_total,
                        "table_fraction": 0.0,
                    }
                )
            scan = scan_table(
                spec,
                db_dir=db_dir,
                status=status,
                run_id=None,
                target_node=SYNC_TARGET_NODE,
                progress=table_scan_progress(progress, spec=spec, rows_total=rows_total),
            )
            if progress is not None:
                progress(
                    {
                        "event": "table_scan_done",
                        "table": spec.name,
                        "table_index": index,
                        "tables_total": len(specs),
                        "rows_scanned": scan.rows_scanned,
                        "rows_total": rows_total,
                        "rows_staged": scan.rows_staged,
                        "table_fraction": 0.85,
                    }
                )
            db_path = source_db_path(db_dir, spec.source_db)
            sqlite_types = sqlite_table_column_types(db_path, spec.source_table)
            blob_columns = blob_columns_for(scan.columns, sqlite_types, scan.changed_rows)
            staging_table = staging_table_name(safe_package_id, spec)
            csv_path = staging_dir / f"{spec.name}.csv"
            if progress is not None:
                progress(
                    {
                        "event": "csv_write_start",
                        "table": spec.name,
                        "table_index": index,
                        "tables_total": len(specs),
                        "rows_staged": scan.rows_staged,
                        "csv_path": str(csv_path),
                        "table_fraction": 0.9,
                    }
                )
            write_staging_csv(csv_path, scan.columns, scan.changed_rows)
            purge_candidates: tuple[PurgeCandidate, ...] = ()
            if include_purge and status is not None:
                current_keys = source_key_jsons(spec, db_dir=db_dir)
                purge_candidates = tuple(
                    PurgeCandidate(table_name=spec.name, logical_key_json=key)
                    for key in status.successful_key_jsons(table_name=spec.name, target_node=SYNC_TARGET_NODE)
                    if key not in current_keys
                )
            staged_tables.append(
                StagedTable(
                    spec=spec,
                    columns=scan.columns,
                    sqlite_types=sqlite_types,
                    blob_columns=blob_columns,
                    staging_table=staging_table,
                    csv_path=csv_path,
                    rows_scanned=scan.rows_scanned,
                    rows_skipped=scan.rows_skipped,
                    rows_staged=scan.rows_staged,
                    purge_candidates=purge_candidates,
                )
            )
            if progress is not None:
                progress(
                    {
                        "event": "table_done",
                        "table": spec.name,
                        "table_index": index,
                        "tables_total": len(specs),
                        "rows_scanned": scan.rows_scanned,
                        "rows_staged": scan.rows_staged,
                        "purge_candidates": len(purge_candidates),
                        "csv_path": str(csv_path),
                        "table_fraction": 1.0,
                    }
                )
    finally:
        if status is not None:
            status.close()

    prepare_sql = generate_prepare_staging_sql(staged_tables, schema=schema)
    sync_sql = generate_package_sync_sql(
        staged_tables,
        schema=schema,
        include_purge=include_purge,
        transaction_scope=transaction_scope,
        rollback_mode=rollback_mode,
    )
    load_script = generate_load_staging_script(staged_tables, schema=schema)
    manifest = {
        "package_id": safe_package_id,
        "generated_at": utc_now(),
        "source_manifest_hash": source_manifest_hash(db_dir),
        "status_db": str(status_db),
        "status_db_exists": Path(status_db).exists(),
        "schema": schema,
        "transaction_scope": transaction_scope,
        "rollback_mode": rollback_mode,
        "include_purge": include_purge,
        "tables": [staged_table_manifest(table, output_dir) for table in staged_tables],
        "totals": {
            "rows_scanned": sum(table.rows_scanned for table in staged_tables),
            "rows_skipped": sum(table.rows_skipped for table in staged_tables),
            "rows_staged": sum(table.rows_staged for table in staged_tables),
            "purge_candidates": sum(len(table.purge_candidates) for table in staged_tables),
        },
    }

    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    (output_dir / "prepare_staging.sql").write_text(prepare_sql, encoding="utf-8")
    (output_dir / "sync.sql").write_text(sync_sql, encoding="utf-8")
    load_path = output_dir / "load_staging.sh"
    load_path.write_text(load_script, encoding="utf-8")
    load_path.chmod(0o755)
    if progress is not None:
        progress(
            {
                "event": "package_done",
                "output_dir": str(output_dir),
                "tables_total": len(staged_tables),
                "rows_staged": sum(table.rows_staged for table in staged_tables),
            }
        )

    return {
        "mode": "generate-sql",
        "state": "SUCCEEDED",
        "output_dir": str(output_dir),
        "manifest_path": str(output_dir / "manifest.json"),
        "sync_sql_path": str(output_dir / "sync.sql"),
        "prepare_staging_sql_path": str(output_dir / "prepare_staging.sql"),
        "load_staging_path": str(load_path),
        **manifest,
    }


def staged_table_manifest(table: StagedTable, output_dir: Path) -> dict[str, Any]:
    return {
        "table": table.spec.name,
        "source_db": table.spec.source_db,
        "source_table": table.spec.source_table,
        "target_table": table.spec.target_table,
        "staging_table": table.staging_table,
        "csv": str(table.csv_path.relative_to(output_dir)),
        "columns": list(table.columns),
        "blob_columns": list(table.blob_columns),
        "rows_scanned": table.rows_scanned,
        "rows_skipped": table.rows_skipped,
        "rows_staged": table.rows_staged,
        "purge_candidates": len(table.purge_candidates),
    }


def rollback_test_phase(
    *,
    config: ReplConfig,
    db_dir: Path,
    status_db: Path,
    specs: Sequence[TableSpec],
    schema: str,
    output_root: Path,
    vector_table: str,
    include_full: bool,
    include_purge: bool,
    transaction_scope: str,
    package_prefix: str | None,
    execute: bool,
    sqlcmd_bin: str,
    mssql_config: Path | None,
    package_runner: Callable[..., Mapping[str, Any]] | None = None,
    progress: ProgressCallback | None = None,
) -> dict[str, Any]:
    package_prefix = safe_sql_token(package_prefix or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"))
    output_root = Path(output_root)
    output_root.mkdir(parents=True, exist_ok=True)
    status_snapshot = status_report(status_db)
    phase_definitions = rollback_phase_definitions(
        specs=tuple(specs),
        vector_table=vector_table,
        include_full=include_full,
        include_purge=include_purge,
    )
    runner = package_runner or run_rollback_package
    phases: list[dict[str, Any]] = []
    state = "SUCCEEDED" if execute else "GENERATED"
    total_units = sum(len(tuple(phase["specs"])) for phase in phase_definitions) or 1
    completed_units = 0
    for phase_index, phase in enumerate(phase_definitions, start=1):
        phase_name = str(phase["name"])
        phase_specs = tuple(phase["specs"])
        phase_output_dir = output_root / phase_name
        if progress is not None:
            progress(
                {
                    "event": "phase_start",
                    "phase": phase_name,
                    "phase_index": phase_index,
                    "phases_total": len(phase_definitions),
                    "tables_total": len(phase_specs),
                    "overall_percent": round((completed_units / total_units) * 100, 1),
                    "phase_percent": 0.0,
                }
            )
        completed_phase_units = 0

        def phase_progress(event: Mapping[str, Any]) -> None:
            nonlocal completed_phase_units
            if progress is None:
                return
            event_dict = dict(event)
            table_fraction = float(event_dict.get("table_fraction") or 0.0)
            phase_units = len(phase_specs) or 1
            overall_units = completed_units + completed_phase_units + table_fraction
            event_dict.update(
                {
                    "phase": phase_name,
                    "phase_index": phase_index,
                    "phases_total": len(phase_definitions),
                    "phase_percent": round(min(100.0, ((completed_phase_units + table_fraction) / phase_units) * 100), 1),
                    "overall_percent": round(min(100.0, (overall_units / total_units) * 100), 1),
                }
            )
            progress(event_dict)
            if event.get("event") == "table_done":
                completed_phase_units = min(phase_units, completed_phase_units + 1)

        package_report = generate_sql_package(
            db_dir=db_dir,
            status_db=status_db,
            specs=phase_specs,
            schema=schema,
            output_dir=phase_output_dir,
            include_purge=bool(phase["include_purge"]),
            transaction_scope=transaction_scope,
            rollback_mode="variable",
            package_id=f"{package_prefix}_{phase_name}",
            progress=phase_progress if progress is not None else None,
        )
        checks = rollback_package_checks(
            package_report,
            phase_name=phase_name,
            include_purge=bool(phase["include_purge"]),
        )
        if checks["state"] != "OK":
            state = "FAILED"
        phase_report: dict[str, Any] = {
            "phase": phase_name,
            "description": phase["description"],
            "tables": [spec.name for spec in phase_specs],
            "include_purge": bool(phase["include_purge"]),
            "output_dir": package_report["output_dir"],
            "manifest_path": package_report["manifest_path"],
            "sync_sql_path": package_report["sync_sql_path"],
            "load_staging_path": package_report["load_staging_path"],
            "totals": package_report["totals"],
            "checks": checks,
            "commands": rollback_operator_commands(
                config=config,
                sqlcmd_bin=sqlcmd_bin,
                package_report=package_report,
                mssql_config=mssql_config,
            ),
        }
        if execute:
            if progress is not None:
                progress(
                    {
                        "event": "phase_execute_start",
                        "phase": phase_name,
                        "phase_index": phase_index,
                        "phases_total": len(phase_definitions),
                        "overall_percent": round(((completed_units + len(phase_specs)) / total_units) * 100, 1),
                        "phase_percent": 100.0,
                    }
                )
            execution = runner(
                config=config,
                schema=schema,
                specs=phase_specs,
                package_report=package_report,
                sqlcmd_bin=sqlcmd_bin,
                mssql_config=mssql_config,
            )
            phase_report["execution"] = dict(execution)
            if execution.get("state") != "SUCCEEDED":
                state = "FAILED"
            if progress is not None:
                progress(
                    {
                        "event": "phase_execute_done",
                        "phase": phase_name,
                        "phase_index": phase_index,
                        "phases_total": len(phase_definitions),
                        "execution_state": execution.get("state"),
                        "counts_unchanged": execution.get("counts_unchanged"),
                        "overall_percent": round(((completed_units + len(phase_specs)) / total_units) * 100, 1),
                        "phase_percent": 100.0,
                    }
                )
        phases.append(phase_report)
        completed_units += len(phase_specs)
        if progress is not None:
            progress(
                {
                    "event": "phase_done",
                    "phase": phase_name,
                    "phase_index": phase_index,
                    "phases_total": len(phase_definitions),
                    "checks_state": checks["state"],
                    "overall_percent": round((completed_units / total_units) * 100, 1),
                    "phase_percent": 100.0,
                }
            )
    return {
        "mode": "rollback-test",
        "state": state,
        "execute": execute,
        "output_root": str(output_root),
        "schema": schema,
        "database": config.database,
        "publisher": config.publisher,
        "source_manifest_hash": source_manifest_hash(db_dir),
        "status_db": str(status_db),
        "status_db_exists": bool(status_snapshot.get("exists")),
        "status_snapshot": status_snapshot,
        "transaction_scope": transaction_scope,
        "rollback_mode": "variable",
        "include_purge": include_purge,
        "phases": phases,
    }


def rollback_phase_definitions(
    *,
    specs: Sequence[TableSpec],
    vector_table: str,
    include_full: bool,
    include_purge: bool,
) -> list[dict[str, Any]]:
    selected_by_name = {spec.name: spec for spec in specs}
    phases: list[dict[str, Any]] = []
    if "metadata" in selected_by_name:
        phases.append(
            {
                "name": "metadata",
                "description": "Small metadata-only rollback validation.",
                "specs": (selected_by_name["metadata"],),
                "include_purge": False,
            }
        )
    vector_spec = selected_specs((vector_table,))[0]
    if vector_spec.name in selected_by_name:
        phases.append(
            {
                "name": "vector",
                "description": f"Focused vector/BLOB rollback validation using {vector_spec.name}.",
                "specs": (vector_spec,),
                "include_purge": False,
            }
        )
    if include_full:
        phase_name = "full" if len(specs) == len(TABLE_SPECS) else "selected"
        phases.append(
            {
                "name": phase_name,
                "description": "Full selected-table rollback validation.",
                "specs": tuple(specs),
                "include_purge": False,
            }
        )
    if include_purge:
        phases.append(
            {
                "name": "purge",
                "description": "Explicit purge rollback validation; DELETE logic is rolled back.",
                "specs": tuple(specs),
                "include_purge": True,
            }
        )
    if not phases:
        raise SyncError("rollback-test has no phases after table filtering; include metadata/vector/full or remove --skip-full")
    return phases


def rollback_package_checks(package_report: Mapping[str, Any], *, phase_name: str, include_purge: bool) -> dict[str, Any]:
    sync_sql = Path(str(package_report["sync_sql_path"])).read_text(encoding="utf-8")
    manifest = json.loads(Path(str(package_report["manifest_path"])).read_text(encoding="utf-8"))
    tables = manifest.get("tables", [])
    checks = [
        {
            "name": "rollback_default_enabled",
            "passed": "DECLARE @RollbackOnly bit = 1" in sync_sql,
            "detail": "Generated SQL defaults to rollback validation mode.",
        },
        {
            "name": "has_rollback_transaction",
            "passed": "ROLLBACK TRANSACTION" in sync_sql,
            "detail": "Generated SQL closes validation transactions with ROLLBACK TRANSACTION.",
        },
        {
            "name": "has_commit_transaction",
            "passed": "COMMIT TRANSACTION" in sync_sql,
            "detail": "Generated SQL can be switched to commit mode after validation.",
        },
        {
            "name": "has_try_catch_guard",
            "passed": "BEGIN TRY" in sync_sql and "BEGIN CATCH" in sync_sql and "THROW;" in sync_sql,
            "detail": "Generated SQL has TRY/CATCH rollback guards.",
        },
        {
            "name": "purge_sql_opt_in",
            "passed": include_purge or "DELETE target" not in sync_sql,
            "detail": "DELETE blocks are absent unless the purge phase is explicitly requested.",
        },
    ]
    if phase_name == "metadata":
        checks.append(
            {
                "name": "metadata_included",
                "passed": any(table.get("table") == "metadata" for table in tables),
                "detail": "The metadata phase stages and MERGEs tmp_metadata.",
            }
        )
    if phase_name == "vector":
        checks.extend(
            [
                {
                    "name": "vector_blob_column_detected",
                    "passed": any(table.get("blob_columns") for table in tables),
                    "detail": "The vector phase detected at least one BLOB/vector column.",
                },
                {
                    "name": "vector_varbinary_conversion",
                    "passed": "CONVERT(VARBINARY(MAX)" in sync_sql,
                    "detail": "The vector phase converts staged hex text to VARBINARY(MAX).",
                },
            ]
        )
    return {
        "state": "OK" if all(bool(check["passed"]) for check in checks) else "FAILED",
        "checks": checks,
    }


def rollback_operator_commands(
    *,
    config: ReplConfig,
    sqlcmd_bin: str,
    package_report: Mapping[str, Any],
    mssql_config: Path | None,
) -> dict[str, str]:
    sqlcmd_args = sqlcmd_args_for_config(config)
    password_env = f"SQLCMDPASSWORD=\"$(cat {shlex.quote(str(config.password_file))})\""
    load_parts = [
        password_env,
        f"SQLCMD_ARGS='{sqlcmd_args_env_value(sqlcmd_args)}'",
    ]
    if mssql_config is not None:
        load_parts.append(f"MSSQL_CONFIG={shlex.quote(str(mssql_config))}")
    load_parts.append("./load_staging.sh")
    sync_parts = [
        password_env,
        shlex.quote(sqlcmd_bin),
        *[shlex.quote(part) for part in sqlcmd_args],
        "-i",
        shlex.quote(str(package_report["sync_sql_path"])),
    ]
    return {
        "working_dir": str(package_report["output_dir"]),
        "load_staging": " ".join(load_parts),
        "run_rollback_sql": " ".join(sync_parts),
    }


def run_rollback_package(
    *,
    config: ReplConfig,
    schema: str,
    specs: Sequence[TableSpec],
    package_report: Mapping[str, Any],
    sqlcmd_bin: str,
    mssql_config: Path | None,
) -> dict[str, Any]:
    output_dir = Path(str(package_report["output_dir"]))
    load_path = Path(str(package_report["load_staging_path"]))
    sync_sql_path = Path(str(package_report["sync_sql_path"]))
    try:
        before_counts = fetch_counts_for_specs(config=config, schema=schema, specs=specs)
        env = rollback_sqlcmd_env(config=config, mssql_config=mssql_config)
        load_result = run_external_command([str(load_path)], cwd=output_dir, env=env)
        sync_result: dict[str, Any] | None = None
        if int(load_result["returncode"]) == 0:
            sync_result = run_external_command(
                [sqlcmd_bin, *sqlcmd_args_for_config(config), "-i", str(sync_sql_path)],
                cwd=output_dir,
                env=env,
            )
        after_counts = fetch_counts_for_specs(config=config, schema=schema, specs=specs)
        counts_unchanged = before_counts == after_counts
        state = "SUCCEEDED" if int(load_result["returncode"]) == 0 and sync_result and int(sync_result["returncode"]) == 0 and counts_unchanged else "FAILED"
        return {
            "state": state,
            "counts_before": before_counts,
            "counts_after": after_counts,
            "counts_unchanged": counts_unchanged,
            "load_staging": load_result,
            "run_rollback_sql": sync_result,
        }
    except Exception as exc:
        return {
            "state": "FAILED",
            "error": str(exc),
        }


def fetch_counts_for_specs(*, config: ReplConfig, schema: str, specs: Sequence[TableSpec]) -> dict[str, int]:
    connection = None
    try:
        connection = connect(config.nodes[config.publisher], config, database=config.database)
        return {spec.name: fetch_table_count(connection, schema, spec.target_table) for spec in specs}
    finally:
        if connection is not None:
            close_connection(connection)


def rollback_sqlcmd_env(*, config: ReplConfig, mssql_config: Path | None) -> dict[str, str]:
    env = dict(os.environ)
    env.setdefault("SQLCMDPASSWORD", read_password(config.password_file))
    env["SQLCMD_ARGS"] = sqlcmd_args_env_value(sqlcmd_args_for_config(config))
    if mssql_config is not None:
        env["MSSQL_CONFIG"] = str(mssql_config)
    return env


def sqlcmd_args_for_config(config: ReplConfig) -> list[str]:
    publisher = config.nodes[config.publisher]
    return ["-S", agent_endpoint_for(publisher), "-U", "sa", "-C", "-d", config.database, "-b"]


def sqlcmd_args_env_value(args: Sequence[str]) -> str:
    for arg in args:
        if any(char.isspace() for char in arg):
            raise SyncError(f"sqlcmd argument contains whitespace and cannot be passed via SQLCMD_ARGS: {arg!r}")
    return " ".join(args)


def run_external_command(command: Sequence[str], *, cwd: Path, env: Mapping[str, str]) -> dict[str, Any]:
    result = subprocess.run(
        list(command),
        cwd=cwd,
        env=dict(env),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    return {
        "command": shlex.join(str(part) for part in command),
        "returncode": result.returncode,
        "stdout_tail": tail_text(result.stdout),
        "stderr_tail": tail_text(result.stderr),
    }


def tail_text(value: str, *, limit: int = 4000) -> str:
    if len(value) <= limit:
        return value
    return value[-limit:]


def stderr_progress_reporter(event: Mapping[str, Any]) -> None:
    print(format_progress_event(event), file=sys.stderr, flush=True)


def format_progress_event(event: Mapping[str, Any]) -> str:
    overall = float(event.get("overall_percent") or 0.0)
    phase = event.get("phase")
    event_name = str(event.get("event") or "progress")
    prefix = f"[WGD rollback-test {overall:5.1f}%]"
    if event_name == "phase_start":
        return f"{prefix} phase {event.get('phase_index')}/{event.get('phases_total')} {phase}: start ({event.get('tables_total')} table(s))"
    if event_name == "table_start":
        return f"{prefix} phase={phase} table {event.get('table_index')}/{event.get('tables_total')} {event.get('table')}: start rows_total={event.get('rows_total')}"
    if event_name == "table_scan_progress":
        rows_total = event.get("rows_total")
        scanned = event.get("rows_scanned")
        if rows_total:
            return f"{prefix} phase={phase} table={event.get('table')}: scanned {scanned}/{rows_total} row(s)"
        return f"{prefix} phase={phase} table={event.get('table')}: scanned {scanned} row(s)"
    if event_name == "table_scan_done":
        return (
            f"{prefix} phase={phase} table={event.get('table')}: scan done "
            f"scanned={event.get('rows_scanned')} staged={event.get('rows_staged')}"
        )
    if event_name == "csv_write_start":
        return f"{prefix} phase={phase} table={event.get('table')}: writing staging CSV rows={event.get('rows_staged')}"
    if event_name == "table_done":
        return (
            f"{prefix} phase={phase} table={event.get('table')}: done "
            f"scanned={event.get('rows_scanned')} staged={event.get('rows_staged')} purge_candidates={event.get('purge_candidates')}"
        )
    if event_name == "package_start":
        return f"{prefix} phase={phase}: package start rows_total={event.get('rows_total')} tables={event.get('tables_total')}"
    if event_name == "package_done":
        return f"{prefix} phase={phase}: package files written rows_staged={event.get('rows_staged')}"
    if event_name == "phase_execute_start":
        return f"{prefix} phase={phase}: executing rollback validation"
    if event_name == "phase_execute_done":
        return (
            f"{prefix} phase={phase}: execution {event.get('execution_state')} "
            f"counts_unchanged={event.get('counts_unchanged')}"
        )
    if event_name == "phase_done":
        return f"{prefix} phase {event.get('phase_index')}/{event.get('phases_total')} {phase}: done checks={event.get('checks_state')}"
    return f"{prefix} {event_name}: {dict(event)}"


def write_staging_csv(csv_path: Path, columns: Sequence[str], rows: Sequence[SourceRow]) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(columns)
        for row in rows:
            writer.writerow([csv_cell(row.values.get(column)) for column in columns])


def csv_cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, memoryview):
        value = value.tobytes()
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).hex().upper()
    return str(value)


def staging_table_name(package_id: str, spec: TableSpec) -> str:
    suffix = safe_sql_token(spec.target_short_name)
    name = f"wgd_stage_{package_id}_{suffix}"
    if len(name) > 120:
        digest = hashlib.sha1(name.encode("utf-8")).hexdigest()[:10]
        name = f"wgd_stage_{package_id[:40]}_{suffix[:50]}_{digest}"
    return name


def safe_sql_token(value: str) -> str:
    token = "".join(char if char.isalnum() or char == "_" else "_" for char in value)
    token = token.strip("_")
    if not token:
        token = "pkg"
    if token[0].isdigit():
        token = "p_" + token
    return token


def transaction_name_for(prefix: str, table_name: str) -> str:
    token = safe_sql_token(f"{prefix}_{table_name}")
    return token[:32]


def generate_prepare_staging_sql(staged_tables: Sequence[StagedTable], *, schema: str) -> str:
    lines = [
        "-- Generated WGD staging table setup.",
        "SET XACT_ABORT ON;",
        "",
    ]
    for table in staged_tables:
        table_name = qualified_sql_name(schema, table.staging_table)
        lines.extend(
            [
                f"IF OBJECT_ID(N'{qualified_sql_literal_name(schema, table.staging_table)}', N'U') IS NOT NULL",
                f"    DROP TABLE {table_name};",
                "GO",
                f"CREATE TABLE {table_name} (",
                "    " + ",\n    ".join(f"{quote_sql_identifier(column)} NVARCHAR(MAX) NULL" for column in table.columns),
                ");",
                "GO",
                "",
            ]
        )
    return "\n".join(lines)


def generate_load_staging_script(staged_tables: Sequence[StagedTable], *, schema: str) -> str:
    lines = [
        "#!/usr/bin/env bash",
        "set -euo pipefail",
        'SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"',
        'PYTHON_BIN="${PYTHON_BIN:-python3}"',
        f'LOADER="${{MSSQL_CSV_LOADER:-{ROOT / "mssqlt" / "mssql_csv_loader.py"}}}"',
        'CONFIG="${MSSQL_CONFIG:-$HOME/.config/mssqltop/config.ini}"',
        'SQLCMD_BIN="${SQLCMD_BIN:-sqlcmd}"',
        "",
        'if [ -n "${SQLCMD_ARGS:-}" ]; then',
        '  "$SQLCMD_BIN" $SQLCMD_ARGS -i "$SCRIPT_DIR/prepare_staging.sql"',
        "else",
        '  echo "SQLCMD_ARGS is not set; run prepare_staging.sql manually before loading staging CSVs." >&2',
        "fi",
        "",
    ]
    for table in staged_tables:
        lines.extend(
            [
                f'echo "Loading {table.spec.name} -> {schema}.{table.staging_table}"',
                f'"$PYTHON_BIN" "$LOADER" --config "$CONFIG" --truncate-table --batch-size 10000 --csv "$SCRIPT_DIR/staging/{table.spec.name}.csv" --table "{schema}.{table.staging_table}"',
                "",
            ]
        )
    return "\n".join(lines)


def generate_package_sync_sql(
    staged_tables: Sequence[StagedTable],
    *,
    schema: str,
    include_purge: bool,
    transaction_scope: str,
    rollback_mode: str,
) -> str:
    lines = [
        "-- Generated incremental WGD sync SQL.",
        "-- Default rollback mode validates MERGE/DELETE operations without committing.",
    ]
    if transaction_scope == "whole-package":
        lines.extend(
            [
                "SET XACT_ABORT ON;",
                rollback_variable_sql(rollback_mode),
                "",
            ]
        )
        transaction_name = transaction_name_for("WGD", "package")
        lines.extend(
            [
                "BEGIN TRY",
                f"    BEGIN TRANSACTION {transaction_name};",
            ]
        )
        for table in staged_tables:
            lines.extend(indent_lines(generate_table_operation_sql(table, schema=schema, include_purge=include_purge, wrap_transaction=False), "    "))
        lines.extend(
            [
                "    IF @RollbackOnly = 1",
                "    BEGIN",
                f"        ROLLBACK TRANSACTION {transaction_name};",
                "        PRINT N'Rolled back WGD package transaction.';",
                "    END",
                "    ELSE",
                "    BEGIN",
                f"        COMMIT TRANSACTION {transaction_name};",
                "        PRINT N'Committed WGD package transaction.';",
                "    END",
                "END TRY",
                "BEGIN CATCH",
                "    IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;",
                "    THROW;",
                "END CATCH;",
                "GO",
                "",
            ]
        )
    else:
        for table in staged_tables:
            scope_label = "batch" if transaction_scope == "per-batch" else "table"
            lines.extend(
                generate_table_operation_sql(
                    table,
                    schema=schema,
                    include_purge=include_purge,
                    wrap_transaction=True,
                    scope_label=scope_label,
                    rollback_mode=rollback_mode,
                )
            )
            lines.extend(["GO", ""])
    lines.extend(generate_cleanup_sql(staged_tables, schema=schema))
    return "\n".join(lines)


def rollback_variable_sql(rollback_mode: str) -> str:
    if rollback_mode == "commit":
        return "DECLARE @RollbackOnly bit = 0;"
    if rollback_mode == "always-rollback":
        return "DECLARE @RollbackOnly bit = 1;"
    return "DECLARE @RollbackOnly bit = 1; -- set to 0 to commit after validation"


def generate_table_operation_sql(
    table: StagedTable,
    *,
    schema: str,
    include_purge: bool,
    wrap_transaction: bool,
    scope_label: str = "table",
    rollback_mode: str = "variable",
) -> list[str]:
    spec = table.spec
    token = safe_sql_token(spec.name)
    transaction_name = transaction_name_for("WGD", spec.name if scope_label == "table" else f"{spec.name}_{scope_label}")
    lines: list[str] = []
    if wrap_transaction:
        lines.extend(
            [
                "SET XACT_ABORT ON;",
                rollback_variable_sql(rollback_mode),
                "BEGIN TRY",
                f"    BEGIN TRANSACTION {transaction_name};",
            ]
        )
        body_indent = "    "
    else:
        body_indent = ""
    body = [
        f"PRINT N'WGD sync {spec.name}: {table.rows_staged} staged row(s).';",
        *generate_staging_guard_sql(table, schema=schema),
        f"DECLARE @WgdActions_{token} TABLE ([merge_action] NVARCHAR(10) NOT NULL);",
        generate_package_merge_sql(table, schema=schema, action_table=f"@WgdActions_{token}"),
        f"SELECT N'{spec.name}' AS [table_name], [merge_action], COUNT_BIG(*) AS [row_count] FROM @WgdActions_{token} GROUP BY [merge_action];",
    ]
    if include_purge and table.purge_candidates:
        body.extend(generate_package_purge_sql(table, schema=schema))
    lines.extend(indent_lines(body, body_indent))
    if wrap_transaction:
        lines.extend(
            [
                "    IF @RollbackOnly = 1",
                "    BEGIN",
                f"        ROLLBACK TRANSACTION {transaction_name};",
                f"        PRINT N'Rolled back WGD {scope_label} transaction for {spec.name}.';",
                "    END",
                "    ELSE",
                "    BEGIN",
                f"        COMMIT TRANSACTION {transaction_name};",
                f"        PRINT N'Committed WGD {scope_label} transaction for {spec.name}.';",
                "    END",
                "END TRY",
                "BEGIN CATCH",
                "    IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;",
                "    THROW;",
                "END CATCH;",
            ]
        )
    return lines


def generate_staging_guard_sql(table: StagedTable, *, schema: str) -> list[str]:
    token = safe_sql_token(table.spec.name)
    stage = qualified_sql_name(schema, table.staging_table)
    guards = [
        *generate_staging_create_guard_sql(table, schema=schema),
        f"IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'{qualified_sql_literal_name(schema, table.staging_table)}') AND name = N'{table.columns[0]}') THROW 51101, 'Unexpected WGD staging shape for {table.staging_table}', 1;",
        f"DECLARE @ExpectedRows_{token} BIGINT = {table.rows_staged};",
        f"DECLARE @ActualRows_{token} BIGINT;",
        f"SELECT @ActualRows_{token} = COUNT_BIG(*) FROM {stage};",
        f"IF @ActualRows_{token} <> @ExpectedRows_{token} THROW 51102, 'Unexpected WGD staging row count for {table.staging_table}', 1;",
    ]
    for key_column in table.spec.key_columns:
        guards.append(
            f"IF EXISTS (SELECT 1 FROM {stage} WHERE {quote_sql_identifier(key_column)} IS NULL OR LTRIM(RTRIM(CONVERT(NVARCHAR(MAX), {quote_sql_identifier(key_column)}))) = N'') THROW 51103, 'Null WGD staging key in {table.staging_table}.{key_column}', 1;"
        )
    return guards


def generate_staging_create_guard_sql(table: StagedTable, *, schema: str) -> list[str]:
    table_name = qualified_sql_name(schema, table.staging_table)
    return [
        f"IF OBJECT_ID(N'{qualified_sql_literal_name(schema, table.staging_table)}', N'U') IS NULL",
        "BEGIN",
        f"    CREATE TABLE {table_name} (",
        "        " + ",\n        ".join(f"{quote_sql_identifier(column)} NVARCHAR(MAX) NULL" for column in table.columns),
        "    );",
        "END;",
    ]


def generate_package_merge_sql(table: StagedTable, *, schema: str, action_table: str) -> str:
    spec = table.spec
    target = qualified_sql_name(schema, spec.target_table)
    source_cte = package_source_cte(table, schema=schema)
    on_clause = key_match_sql("target", "source", spec.key_columns)
    update_columns = [
        column for column in table.columns
        if column not in spec.key_columns and column != spec.surrogate_column
    ]
    update_clause = ", ".join(
        f"{quote_sql_identifier(column)} = source.{quote_sql_identifier(column)}"
        for column in update_columns
    )
    if not update_clause:
        update_clause = f"{quote_sql_identifier(spec.key_columns[0])} = target.{quote_sql_identifier(spec.key_columns[0])}"
    insert_columns = list(table.columns)
    insert_values = [
        "source.[__sync_effective_id]" if column == spec.surrogate_column else f"source.{quote_sql_identifier(column)}"
        for column in insert_columns
    ]
    return "\n".join(
        [
            source_cte,
            f"MERGE {target} WITH (HOLDLOCK) AS target",
            "USING source",
            f"ON {on_clause}",
            "WHEN MATCHED THEN",
            f"    UPDATE SET {update_clause}",
            "WHEN NOT MATCHED BY TARGET THEN",
            f"    INSERT ({', '.join(quote_sql_identifier(column) for column in insert_columns)})",
            f"    VALUES ({', '.join(insert_values)})",
            f"OUTPUT $action INTO {action_table};",
        ]
    )


def package_source_cte(table: StagedTable, *, schema: str) -> str:
    spec = table.spec
    target = qualified_sql_name(schema, spec.target_table)
    stage = qualified_sql_name(schema, table.staging_table)
    selected_columns = ",\n        ".join(package_source_expression(table, column) for column in table.columns)
    if spec.surrogate_column is None:
        return f"WITH source AS (\n    SELECT\n        {selected_columns}\n    FROM {stage} AS s\n)"
    key_order = ", ".join(f"s.{quote_sql_identifier(column)}" for column in spec.key_columns)
    matched_join = key_match_sql("matched", "s", spec.key_columns)
    collision_join = (
        f"collision.{quote_sql_identifier(spec.surrogate_column)} = TRY_CONVERT(BIGINT, s.{quote_sql_identifier(spec.surrogate_column)}) "
        f"AND NOT ({key_match_sql('collision', 's', spec.key_columns)})"
    )
    surrogate = quote_sql_identifier(spec.surrogate_column)
    return (
        "WITH source AS (\n"
        f"    SELECT\n        {selected_columns},\n"
        f"        CASE\n"
        f"            WHEN matched.{surrogate} IS NOT NULL THEN matched.{surrogate}\n"
        f"            WHEN TRY_CONVERT(BIGINT, s.{surrogate}) IS NULL THEN max_ids.max_id + ROW_NUMBER() OVER (ORDER BY {key_order})\n"
        f"            WHEN collision.{surrogate} IS NULL THEN TRY_CONVERT(BIGINT, s.{surrogate})\n"
        f"            ELSE max_ids.max_id + ROW_NUMBER() OVER (ORDER BY {key_order})\n"
        f"        END AS {quote_sql_identifier('__sync_effective_id')}\n"
        f"    FROM {stage} AS s\n"
        f"    LEFT JOIN {target} AS matched ON {matched_join}\n"
        f"    LEFT JOIN {target} AS collision ON {collision_join}\n"
        f"    CROSS JOIN (\n"
        f"        SELECT CASE WHEN target_max.max_id > stage_max.max_id THEN target_max.max_id ELSE stage_max.max_id END AS max_id\n"
        f"        FROM (SELECT ISNULL(MAX({surrogate}), 0) AS max_id FROM {target}) AS target_max\n"
        f"        CROSS JOIN (SELECT ISNULL(MAX(TRY_CONVERT(BIGINT, {surrogate})), 0) AS max_id FROM {stage}) AS stage_max\n"
        f"    ) AS max_ids\n"
        ")"
    )


def package_source_expression(table: StagedTable, column: str) -> str:
    quoted = quote_sql_identifier(column)
    if column in table.blob_columns:
        return (
            f"CASE WHEN s.{quoted} IS NULL OR LTRIM(RTRIM(CONVERT(NVARCHAR(MAX), s.{quoted}))) = N'' "
            f"THEN NULL ELSE CONVERT(VARBINARY(MAX), CONVERT(VARCHAR(MAX), s.{quoted}), 2) END AS {quoted}"
        )
    return f"s.{quoted} AS {quoted}"


def generate_package_purge_sql(table: StagedTable, *, schema: str) -> list[str]:
    spec = table.spec
    token = safe_sql_token(spec.name)
    values_rows = []
    for candidate in table.purge_candidates:
        values = ", ".join(tsql_literal(value) for value in logical_key_values_from_json(candidate.logical_key_json))
        values_rows.append(f"        ({values})")
    key_defs = ", ".join(f"{quote_sql_identifier(column)} NVARCHAR(450)" for column in spec.key_columns)
    join_clause = " AND ".join(f"target.{quote_sql_identifier(column)} = purge_keys.{quote_sql_identifier(column)}" for column in spec.key_columns)
    return [
        f"DECLARE @WgdPurged_{token} TABLE ([deleted] INT NOT NULL);",
        f"DECLARE @WgdPurgeKeys_{token} TABLE ({key_defs});",
        f"INSERT INTO @WgdPurgeKeys_{token} ({', '.join(quote_sql_identifier(column) for column in spec.key_columns)})",
        "VALUES",
        ",\n".join(values_rows) + ";",
        f"DELETE target",
        f"OUTPUT 1 INTO @WgdPurged_{token}",
        f"FROM {qualified_sql_name(schema, spec.target_table)} AS target",
        f"JOIN @WgdPurgeKeys_{token} AS purge_keys ON {join_clause};",
        f"SELECT N'{spec.name}' AS [table_name], N'DELETE' AS [merge_action], COUNT_BIG(*) AS [row_count] FROM @WgdPurged_{token};",
    ]


def generate_cleanup_sql(staged_tables: Sequence[StagedTable], *, schema: str) -> list[str]:
    lines = ["-- Cleanup staging tables after sync validation/apply."]
    for table in staged_tables:
        lines.extend(
            [
                f"IF OBJECT_ID(N'{qualified_sql_literal_name(schema, table.staging_table)}', N'U') IS NOT NULL",
                f"    DROP TABLE {qualified_sql_name(schema, table.staging_table)};",
                "GO",
            ]
        )
    return lines


def indent_lines(lines: str | Sequence[str], prefix: str) -> list[str]:
    if isinstance(lines, str):
        raw_lines = lines.splitlines()
    else:
        raw_lines = []
        for line in lines:
            raw_lines.extend(str(line).splitlines())
    return [prefix + line if line else line for line in raw_lines]


def qualified_sql_literal_name(schema: str, table: str) -> str:
    return f"[{schema.replace(']', ']]')}].[{table.replace(']', ']]')}]"


def tsql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, int):
        return str(value)
    text = str(value)
    return "N'" + text.replace("'", "''") + "'"


def dry_run(*, db_dir: Path, status_db: Path, specs: Sequence[TableSpec], include_purge: bool = False) -> dict[str, Any]:
    status = StatusStore(status_db, create=False) if Path(status_db).exists() else None
    try:
        table_reports = []
        for spec in specs:
            scan = scan_table(spec, db_dir=db_dir, status=status, run_id=None, target_node=SYNC_TARGET_NODE)
            report = scan_to_dict(scan, merge=None)
            if include_purge:
                report["purge_candidates"] = preview_purge_candidate_count(
                    spec,
                    db_dir=db_dir,
                    status=status,
                    target_node=SYNC_TARGET_NODE,
                )
            table_reports.append(report)
        totals = summarize_table_reports(table_reports)
        return {
            "mode": "dry-run",
            "state": "DRY_RUN",
            "include_purge": include_purge,
            "source_manifest_hash": source_manifest_hash(db_dir),
            "tables": table_reports,
            "totals": totals,
        }
    finally:
        if status is not None:
            status.close()


def sync(
    *,
    config: ReplConfig,
    db_dir: Path,
    status_db: Path,
    specs: Sequence[TableSpec],
    schema: str,
    batch_size: int,
    purge_missing: bool = False,
) -> dict[str, Any]:
    source_hash = source_manifest_hash(db_dir)
    with StatusStore(status_db, create=True) as status:
        run_id = status.create_run(mode="sync", source_manifest_hash=source_hash)
        table_reports: list[dict[str, Any]] = []
        state = "SUCCEEDED"
        error: str | None = None
        connection = None
        try:
            publisher = config.nodes[config.publisher]
            connection = connect(publisher, config, database=config.database)
            for spec in specs:
                scan = scan_table(
                    spec,
                    db_dir=db_dir,
                    status=status,
                    run_id=run_id,
                    target_node=SYNC_TARGET_NODE,
                )
                merge_result = MergeResult()
                table_error: str | None = None
                if scan.changed_rows:
                    try:
                        merge_result = merge_scan(
                            connection,
                            spec=spec,
                            columns=scan.columns,
                            rows=scan.changed_rows,
                            schema=schema,
                            batch_size=batch_size,
                        )
                        status.mark_success(
                            table_name=spec.name,
                            rows=scan.changed_rows,
                            target_node=SYNC_TARGET_NODE,
                            run_id=run_id,
                            surrogate_ids_by_key=merge_result.surrogate_ids_by_key,
                        )
                    except Exception as exc:
                        table_error = str(exc)
                        state = "FAILED"
                        merge_result = MergeResult(failed=len(scan.changed_rows))
                status.record_table_run(
                    run_id=run_id,
                    node=SYNC_TARGET_NODE,
                    table_name=spec.name,
                    scan=scan,
                    merge=merge_result,
                    error=table_error,
                )
                table_reports.append(scan_to_dict(scan, merge_result, error=table_error))
                if table_error:
                    raise SyncError(f"{spec.name}: {table_error}")
            purge_results: list[PurgeResult] = []
            if purge_missing:
                purge_results = apply_purge_for_run(
                    connection,
                    status=status,
                    source_run_id=run_id,
                    purge_run_id=run_id,
                    specs=specs,
                    schema=schema,
                    batch_size=batch_size,
                )
                purge_by_table = {result.table_name: result for result in purge_results}
                for report in table_reports:
                    purge_result = purge_by_table.get(str(report["table"]))
                    if purge_result is None:
                        continue
                    report["purge_candidates"] = purge_result.candidates
                    report["rows_purged"] = purge_result.purged
                    report["purge_error"] = purge_result.error or None
                    status.add_table_purge_count(
                        run_id=run_id,
                        table_name=str(report["table"]),
                        node=SYNC_TARGET_NODE,
                        purged=purge_result.purged,
                    )
                    if purge_result.error:
                        raise SyncError(f"{purge_result.table_name} purge: {purge_result.error}")
        except Exception as exc:
            state = "FAILED"
            error = str(exc)
            raise
        finally:
            if connection is not None:
                close_connection(connection)
            status.finish_run(run_id, state=state, error=error)
        totals = summarize_table_reports(table_reports)
        return {
            "mode": "sync",
            "run_id": run_id,
            "state": state,
            "purge_missing": purge_missing,
            "source_manifest_hash": source_hash,
            "tables": table_reports,
            "totals": totals,
        }


def purge(
    *,
    config: ReplConfig,
    status_db: Path,
    specs: Sequence[TableSpec],
    schema: str,
    run_id: int | None,
    batch_size: int,
    confirm_purge: bool,
) -> dict[str, Any]:
    with StatusStore(status_db, create=True) as status:
        source_run_id = run_id or status.latest_successful_run_id()
        if source_run_id is None:
            raise SyncError("no successful sync run is available to purge against")
        candidates_by_table = status.purge_candidates(
            run_id=source_run_id,
            specs=specs,
            target_node=SYNC_TARGET_NODE,
        )
        candidate_results = [
            PurgeResult(table_name=spec.name, candidates=len(candidates_by_table.get(spec.name, ())))
            for spec in order_specs_for_purge(specs)
        ]
        if not confirm_purge:
            return {
                "mode": "purge",
                "state": "DRY_RUN",
                "source_run_id": source_run_id,
                "run_id": None,
                "confirmed": False,
                "tables": [purge_result_to_dict(result) for result in candidate_results],
                "totals": summarize_purge_results(candidate_results),
            }

        purge_run_id = status.create_run(mode="purge", source_manifest_hash=None)
        connection = None
        state = "SUCCEEDED"
        error: str | None = None
        purge_results: list[PurgeResult] = []
        try:
            publisher = config.nodes[config.publisher]
            connection = connect(publisher, config, database=config.database)
            purge_results = apply_purge_for_run(
                connection,
                status=status,
                source_run_id=source_run_id,
                purge_run_id=purge_run_id,
                specs=specs,
                schema=schema,
                batch_size=batch_size,
                precomputed_candidates=candidates_by_table,
                record_table_runs=True,
            )
            for result in purge_results:
                if result.error:
                    raise SyncError(f"{result.table_name} purge: {result.error}")
        except Exception as exc:
            state = "FAILED"
            error = str(exc)
            raise
        finally:
            if connection is not None:
                close_connection(connection)
            status.finish_run(purge_run_id, state=state, error=error)
        return {
            "mode": "purge",
            "state": state,
            "source_run_id": source_run_id,
            "run_id": purge_run_id,
            "confirmed": True,
            "tables": [purge_result_to_dict(result) for result in purge_results],
            "totals": summarize_purge_results(purge_results),
        }


def apply_purge_for_run(
    connection: Any,
    *,
    status: StatusStore,
    source_run_id: int,
    purge_run_id: int,
    specs: Sequence[TableSpec],
    schema: str,
    batch_size: int,
    precomputed_candidates: Mapping[str, Sequence[PurgeCandidate]] | None = None,
    record_table_runs: bool = False,
) -> list[PurgeResult]:
    candidates_by_table = precomputed_candidates or status.purge_candidates(
        run_id=source_run_id,
        specs=specs,
        target_node=SYNC_TARGET_NODE,
    )
    results: list[PurgeResult] = []
    for spec in order_specs_for_purge(specs):
        started_at = utc_now()
        candidates = list(candidates_by_table.get(spec.name, ()))
        purged = 0
        error = ""
        try:
            for batch in chunked(candidates, batch_size):
                purged += execute_delete_batch(connection, spec=spec, schema=schema, candidates=batch)
            status.mark_purged(
                table_name=spec.name,
                candidates=candidates,
                target_node=SYNC_TARGET_NODE,
                purge_run_id=purge_run_id,
            )
        except Exception as exc:
            error = str(exc)
        finished_at = utc_now()
        result = PurgeResult(table_name=spec.name, candidates=len(candidates), purged=purged, error=error)
        if record_table_runs:
            status.record_purge_table_run(
                run_id=purge_run_id,
                node=SYNC_TARGET_NODE,
                table_name=spec.name,
                candidates=len(candidates),
                purged=purged,
                started_at=started_at,
                finished_at=finished_at,
                error=error or None,
            )
        results.append(result)
        if error:
            break
    return results


def execute_delete_batch(
    connection: Any,
    *,
    spec: TableSpec,
    schema: str,
    candidates: Sequence[PurgeCandidate],
) -> int:
    if not candidates:
        return 0
    sql = generate_delete_sql(spec, schema=schema)
    params = [logical_key_values_from_json(candidate.logical_key_json) for candidate in candidates]
    cursor = connection.cursor()
    try:
        try:
            cursor.fast_executemany = True
        except Exception:
            pass
        cursor.executemany(sql, params)
        commit_connection(connection)
        return len(candidates)
    except Exception:
        rollback_connection(connection)
        raise
    finally:
        cursor.close()


def generate_delete_sql(spec: TableSpec, *, schema: str = "dbo") -> str:
    where_clause = " AND ".join(f"{quote_sql_identifier(column)} = ?" for column in spec.key_columns)
    return f"DELETE FROM {qualified_sql_name(schema, spec.target_table)} WHERE {where_clause};"


def order_specs_for_purge(specs: Sequence[TableSpec]) -> tuple[TableSpec, ...]:
    order = {table_name: index for index, table_name in enumerate(PURGE_TABLE_ORDER)}
    return tuple(sorted(specs, key=lambda spec: order.get(spec.name, len(order))))


def merge_scan(
    connection: Any,
    *,
    spec: TableSpec,
    columns: Sequence[str],
    rows: Sequence[SourceRow],
    schema: str,
    batch_size: int,
) -> MergeResult:
    aggregate_actions = Counter()
    surrogate_ids_by_key: dict[str, int | None] = {}
    for batch in chunked(rows, batch_size):
        result_rows = execute_merge_batch(connection, spec=spec, columns=columns, rows=batch, schema=schema)
        for result in result_rows:
            action = str(result.get(SQL_MERGE_ACTION_COLUMN, "")).upper()
            if action:
                aggregate_actions[action] += 1
            key = logical_key_json(spec, [result[column] for column in spec.key_columns])
            surrogate_value = result.get(SQL_SURROGATE_OUTPUT_COLUMN)
            surrogate_ids_by_key[key] = int(surrogate_value) if surrogate_value is not None else None
    return MergeResult(
        inserted=aggregate_actions["INSERT"],
        updated=aggregate_actions["UPDATE"],
        surrogate_ids_by_key=surrogate_ids_by_key,
    )


def execute_merge_batch(
    connection: Any,
    *,
    spec: TableSpec,
    columns: Sequence[str],
    rows: Sequence[SourceRow],
    schema: str,
) -> list[dict[str, Any]]:
    if not rows:
        return []
    stage_table = f"#wgd_sync_{spec.name}_{int(time.time() * 1000)}"
    cursor = connection.cursor()
    try:
        create_stage_table(cursor, schema=schema, target_table=spec.target_table, stage_table=stage_table, columns=columns)
        insert_stage_rows(cursor, stage_table=stage_table, columns=columns, rows=rows)
        merge_sql = generate_merge_sql(spec, columns, schema=schema, stage_table=stage_table)
        cursor.execute(merge_sql)
        description = [str(column[0]) for column in cursor.description]
        output_rows = [dict(zip(description, row)) for row in cursor.fetchall()]
        commit_connection(connection)
        return output_rows
    except Exception:
        rollback_connection(connection)
        raise
    finally:
        try:
            cursor.execute(f"DROP TABLE {quote_sql_identifier(stage_table)}")
        except Exception:
            pass
        cursor.close()


def create_stage_table(
    cursor: Any,
    *,
    schema: str,
    target_table: str,
    stage_table: str,
    columns: Sequence[str],
) -> None:
    select_columns = ", ".join(quote_sql_identifier(column) for column in columns)
    cursor.execute(
        f"SELECT TOP (0) {select_columns} INTO {quote_sql_identifier(stage_table)} "
        f"FROM {qualified_sql_name(schema, target_table)}"
    )


def insert_stage_rows(cursor: Any, *, stage_table: str, columns: Sequence[str], rows: Sequence[SourceRow]) -> None:
    insert_columns = ", ".join(quote_sql_identifier(column) for column in columns)
    placeholders = ", ".join("?" for _ in columns)
    sql = f"INSERT INTO {quote_sql_identifier(stage_table)} ({insert_columns}) VALUES ({placeholders})"
    try:
        cursor.fast_executemany = True
    except Exception:
        pass
    cursor.executemany(sql, [tuple(row.values[column] for column in columns) for row in rows])


def generate_merge_sql(
    spec: TableSpec,
    columns: Sequence[str],
    *,
    schema: str = "dbo",
    stage_table: str = "#wgd_sync_stage",
) -> str:
    validate_spec_columns(spec, columns)
    target = qualified_sql_name(schema, spec.target_table)
    stage = quote_sql_identifier(stage_table)
    target_alias = "target"
    source_alias = "source"
    on_clause = key_match_sql(target_alias, source_alias, spec.key_columns)
    update_columns = [
        column
        for column in columns
        if column not in spec.key_columns and column != spec.surrogate_column
    ]
    update_clause = ", ".join(
        f"{quote_sql_identifier(column)} = {source_alias}.{quote_sql_identifier(column)}"
        for column in update_columns
    )
    if not update_clause:
        update_clause = f"{quote_sql_identifier(spec.key_columns[0])} = {target_alias}.{quote_sql_identifier(spec.key_columns[0])}"

    output_columns = ",\n       ".join(
        f"{source_alias}.{quote_sql_identifier(column)} AS {quote_sql_identifier(column)}"
        for column in spec.key_columns
    )

    if spec.surrogate_column:
        source_sql = surrogate_source_cte(spec, columns, schema=schema, stage_table=stage_table)
        insert_columns = list(columns)
        insert_values = [
            f"{source_alias}.{quote_sql_identifier('__sync_effective_id')}"
            if column == spec.surrogate_column
            else f"{source_alias}.{quote_sql_identifier(column)}"
            for column in insert_columns
        ]
        surrogate_output = f"inserted.{quote_sql_identifier(spec.surrogate_column)}"
    else:
        source_sql = f"{stage} AS {source_alias}"
        insert_columns = list(columns)
        insert_values = [f"{source_alias}.{quote_sql_identifier(column)}" for column in insert_columns]
        surrogate_output = "CAST(NULL AS BIGINT)"

    insert_column_sql = ", ".join(quote_sql_identifier(column) for column in insert_columns)
    insert_value_sql = ", ".join(insert_values)
    return (
        f"{source_sql}\n"
        f"MERGE {target} WITH (HOLDLOCK) AS {target_alias}\n"
        f"USING {source_alias}\n"
        f"ON {on_clause}\n"
        f"WHEN MATCHED THEN\n"
        f"    UPDATE SET {update_clause}\n"
        f"WHEN NOT MATCHED BY TARGET THEN\n"
        f"    INSERT ({insert_column_sql})\n"
        f"    VALUES ({insert_value_sql})\n"
        f"OUTPUT $action AS {quote_sql_identifier(SQL_MERGE_ACTION_COLUMN)},\n"
        f"       {surrogate_output} AS {quote_sql_identifier(SQL_SURROGATE_OUTPUT_COLUMN)},\n"
        f"       {output_columns};"
    )


def surrogate_source_cte(spec: TableSpec, columns: Sequence[str], *, schema: str, stage_table: str) -> str:
    if spec.surrogate_column is None:
        raise SyncError("surrogate_source_cte requires a surrogate column")
    target = qualified_sql_name(schema, spec.target_table)
    stage = quote_sql_identifier(stage_table)
    selected_columns = ",\n        ".join(f"s.{quote_sql_identifier(column)}" for column in columns)
    key_order = ", ".join(f"s.{quote_sql_identifier(column)}" for column in spec.key_columns)
    matched_join = key_match_sql("matched", "s", spec.key_columns)
    collision_join = (
        f"collision.{quote_sql_identifier(spec.surrogate_column)} = s.{quote_sql_identifier(spec.surrogate_column)} "
        f"AND NOT ({key_match_sql('collision', 's', spec.key_columns)})"
    )
    surrogate = quote_sql_identifier(spec.surrogate_column)
    return (
        "WITH source AS (\n"
        f"    SELECT\n        {selected_columns},\n"
        f"        CASE\n"
        f"            WHEN matched.{surrogate} IS NOT NULL THEN matched.{surrogate}\n"
        f"            WHEN s.{surrogate} IS NULL THEN max_ids.max_id + ROW_NUMBER() OVER (ORDER BY {key_order})\n"
        f"            WHEN collision.{surrogate} IS NULL THEN s.{surrogate}\n"
        f"            ELSE max_ids.max_id + ROW_NUMBER() OVER (ORDER BY {key_order})\n"
        f"        END AS {quote_sql_identifier('__sync_effective_id')}\n"
        f"    FROM {stage} AS s\n"
        f"    LEFT JOIN {target} AS matched ON {matched_join}\n"
        f"    LEFT JOIN {target} AS collision ON {collision_join}\n"
        f"    CROSS JOIN (\n"
        f"        SELECT CASE WHEN target_max.max_id > stage_max.max_id THEN target_max.max_id ELSE stage_max.max_id END AS max_id\n"
        f"        FROM (SELECT ISNULL(MAX({surrogate}), 0) AS max_id FROM {target}) AS target_max\n"
        f"        CROSS JOIN (SELECT ISNULL(MAX({surrogate}), 0) AS max_id FROM {stage}) AS stage_max\n"
        f"    ) AS max_ids\n"
        ")"
    )


def key_match_sql(left_alias: str, right_alias: str, key_columns: Sequence[str]) -> str:
    return " AND ".join(
        f"{left_alias}.{quote_sql_identifier(column)} = {right_alias}.{quote_sql_identifier(column)}"
        for column in key_columns
    )


def validate_spec_columns(spec: TableSpec, columns: Sequence[str]) -> None:
    missing_keys = [column for column in spec.key_columns if column not in columns]
    if missing_keys:
        raise SyncError(f"{spec.name} columns missing key column(s): {', '.join(missing_keys)}")
    if spec.surrogate_column and spec.surrogate_column not in columns:
        raise SyncError(f"{spec.name} columns missing surrogate column: {spec.surrogate_column}")


def verify(
    *,
    config: ReplConfig,
    db_dir: Path,
    status_db: Path,
    specs: Sequence[TableSpec],
    schema: str,
    run_id: int | None,
    timeout_seconds: float,
    poll_seconds: float,
) -> dict[str, Any]:
    with StatusStore(status_db, create=True) as status:
        resolved_run_id = run_id or status.latest_successful_run_id()
        if resolved_run_id is None:
            raise SyncError("no successful sync run is available to verify")
        changed_by_table = status.changed_rows_for_run(resolved_run_id, target_node=SYNC_TARGET_NODE)
        started = time.monotonic()
        deadline = time.monotonic() + timeout_seconds
        all_results: list[VerificationResult] = []
        while True:
            lag_seconds = max(0.0, time.monotonic() - started)
            all_results = verify_once(
                config=config,
                db_dir=db_dir,
                specs=specs,
                schema=schema,
                run_id=resolved_run_id,
                changed_by_table=changed_by_table,
                lag_seconds=lag_seconds,
            )
            for result in all_results:
                status.record_verification(run_id=resolved_run_id, result=result)
            if all(result.verification_state == "OK" for result in all_results):
                break
            if time.monotonic() >= deadline:
                break
            time.sleep(poll_seconds)
        state = "OK" if all(result.verification_state == "OK" for result in all_results) else "NOT_OK"
        return {
            "mode": "verify",
            "run_id": resolved_run_id,
            "state": state,
            "results": [verification_to_dict(result) for result in all_results],
        }


def verify_once(
    *,
    config: ReplConfig,
    db_dir: Path,
    specs: Sequence[TableSpec],
    schema: str,
    run_id: int,
    changed_by_table: Mapping[str, Sequence[VerificationRow]],
    lag_seconds: float = 0.0,
) -> list[VerificationResult]:
    publisher = config.nodes[config.publisher]
    publisher_connection = None
    publisher_counts: dict[str, int] = {}
    try:
        publisher_connection = connect(publisher, config, database=config.database)
        for spec in specs:
            publisher_counts[spec.name] = fetch_table_count(publisher_connection, schema, spec.target_table)
    finally:
        if publisher_connection is not None:
            close_connection(publisher_connection)

    results: list[VerificationResult] = []
    for node in config.nodes.values():
        connection = None
        try:
            connection = connect(node, config, database=config.database)
            for spec in specs:
                columns = sqlite_table_columns(source_db_path(db_dir, spec.source_db), spec.source_table)
                result = verify_table_on_connection(
                    connection,
                    node_name=node.name,
                    spec=spec,
                    columns=columns,
                    schema=schema,
                    changed_rows=changed_by_table.get(spec.name, ()),
                    expected_row_count=publisher_counts.get(spec.name),
                    lag_seconds=lag_seconds,
                )
                results.append(result)
        except Exception as exc:
            for spec in specs:
                results.append(
                    VerificationResult(
                        node=node.name,
                        table_name=spec.name,
                        row_count=None,
                        changed_row_verification_count=0,
                        verification_state="ERROR",
                        lag_seconds=lag_seconds,
                        error=str(exc),
                    )
                )
        finally:
            if connection is not None:
                close_connection(connection)
    return results


def verify_table_on_connection(
    connection: Any,
    *,
    node_name: str,
    spec: TableSpec,
    columns: Sequence[str],
    schema: str,
    changed_rows: Sequence[VerificationRow],
    expected_row_count: int | None,
    lag_seconds: float,
) -> VerificationResult:
    try:
        row_count = fetch_table_count(connection, schema, spec.target_table)
        if expected_row_count is not None and row_count != expected_row_count:
            return VerificationResult(
                node=node_name,
                table_name=spec.name,
                row_count=row_count,
                changed_row_verification_count=0,
                verification_state="LAGGING",
                lag_seconds=lag_seconds,
                error=f"row_count {row_count} != publisher {expected_row_count}",
            )
        verified = 0
        for changed in changed_rows:
            key_values = logical_key_values_from_json(changed.logical_key_json)
            target_row = fetch_target_row(
                connection,
                schema=schema,
                target_table=spec.target_table,
                key_columns=spec.key_columns,
                key_values=key_values,
                columns=columns,
            )
            if target_row is None:
                return VerificationResult(
                    node=node_name,
                    table_name=spec.name,
                    row_count=row_count,
                    changed_row_verification_count=verified,
                    verification_state="LAGGING",
                    lag_seconds=lag_seconds,
                    error=f"missing changed row {changed.logical_key_json}",
                )
            target_hash = canonical_row_hash(target_row, columns, spec.surrogate_column)
            if target_hash != changed.row_hash:
                return VerificationResult(
                    node=node_name,
                    table_name=spec.name,
                    row_count=row_count,
                    changed_row_verification_count=verified,
                    verification_state="MISMATCH",
                    lag_seconds=lag_seconds,
                    error=f"hash mismatch for {changed.logical_key_json}",
                )
            verified += 1
        return VerificationResult(
            node=node_name,
            table_name=spec.name,
            row_count=row_count,
            changed_row_verification_count=verified,
            verification_state="OK",
            lag_seconds=lag_seconds,
        )
    except Exception as exc:
        return VerificationResult(
            node=node_name,
            table_name=spec.name,
            row_count=None,
            changed_row_verification_count=0,
            verification_state="ERROR",
            lag_seconds=lag_seconds,
            error=str(exc),
        )


def fetch_table_count(connection: Any, schema: str, target_table: str) -> int:
    cursor = connection.cursor()
    try:
        cursor.execute(f"SELECT COUNT_BIG(*) AS row_count FROM {qualified_sql_name(schema, target_table)}")
        row = cursor.fetchone()
        if row is None:
            raise SyncError(f"row count returned no rows for {target_table}")
        return int(row[0])
    finally:
        cursor.close()


def fetch_target_row(
    connection: Any,
    *,
    schema: str,
    target_table: str,
    key_columns: Sequence[str],
    key_values: Sequence[Any],
    columns: Sequence[str],
) -> dict[str, Any] | None:
    select_columns = ", ".join(quote_sql_identifier(column) for column in columns)
    where_clause = " AND ".join(f"{quote_sql_identifier(column)} = ?" for column in key_columns)
    sql = f"SELECT TOP (1) {select_columns} FROM {qualified_sql_name(schema, target_table)} WHERE {where_clause}"
    cursor = connection.cursor()
    try:
        cursor.execute(sql, *key_values)
        row = cursor.fetchone()
        if row is None:
            return None
        return dict(zip(columns, row))
    finally:
        cursor.close()


def source_manifest_hash(db_dir: Path) -> str | None:
    manifest = Path(db_dir) / "manifest.json"
    if manifest.exists():
        return hashlib.sha256(manifest.read_bytes()).hexdigest()
    parts: list[str] = []
    for source_key, filename in sorted(SOURCE_DB_FILES.items()):
        path = Path(db_dir) / filename
        if path.exists():
            stat = path.stat()
            parts.append(f"{source_key}:{stat.st_size}:{stat.st_mtime_ns}")
    if not parts:
        return None
    return hashlib.sha256("\n".join(parts).encode("utf-8")).hexdigest()


def status_report(status_db: Path) -> dict[str, Any]:
    if not Path(status_db).exists():
        return {"exists": False, "path": str(status_db), "latest_run": None, "tables": [], "verification": []}
    with StatusStore(status_db, create=False) as status:
        report = status.latest_report()
    report["path"] = str(status_db)
    return report


def scan_to_dict(scan: TableScan, merge: MergeResult | None, *, error: str | None = None) -> dict[str, Any]:
    return {
        "table": scan.spec.name,
        "source_db": scan.spec.source_db,
        "source_table": scan.spec.source_table,
        "target_table": scan.spec.target_table,
        "rows_scanned": scan.rows_scanned,
        "rows_skipped": scan.rows_skipped,
        "rows_staged": scan.rows_staged,
        "rows_inserted": merge.inserted if merge else 0,
        "rows_updated": merge.updated if merge else 0,
        "rows_failed": merge.failed if merge else 0,
        "rows_purged": 0,
        "error": error,
    }


def summarize_table_reports(table_reports: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    fields = (
        "rows_scanned",
        "rows_skipped",
        "rows_staged",
        "rows_inserted",
        "rows_updated",
        "rows_failed",
        "rows_purged",
        "purge_candidates",
    )
    return {field: sum(int(report.get(field) or 0) for report in table_reports) for field in fields}


def verification_to_dict(result: VerificationResult) -> dict[str, Any]:
    return {
        "node": result.node,
        "table": result.table_name,
        "row_count": result.row_count,
        "changed_row_verification_count": result.changed_row_verification_count,
        "verification_state": result.verification_state,
        "lag_seconds": result.lag_seconds,
        "error": result.error,
    }


def purge_result_to_dict(result: PurgeResult) -> dict[str, Any]:
    return {
        "table": result.table_name,
        "purge_candidates": result.candidates,
        "rows_purged": result.purged,
        "error": result.error or None,
    }


def summarize_purge_results(results: Sequence[PurgeResult]) -> dict[str, int]:
    return {
        "purge_candidates": sum(result.candidates for result in results),
        "rows_purged": sum(result.purged for result in results),
        "rows_failed": sum(result.candidates for result in results if result.error),
    }


def format_scan_report(report: Mapping[str, Any], output_format: str, *, title: str) -> str:
    if output_format == "json":
        return json.dumps(report, indent=2, sort_keys=True, default=str)
    lines = [
        title,
        f"Mode: {report.get('mode')}",
        f"State: {report.get('state')}",
    ]
    if report.get("run_id") is not None:
        lines.append(f"Run: {report['run_id']}")
    lines.append(f"Source manifest hash: {report.get('source_manifest_hash') or '<none>'}")
    lines.append("")
    lines.append("Tables:")
    for table in report.get("tables", []):
        line = (
            f"  {table['table']}: scanned={table['rows_scanned']} "
            f"skipped={table['rows_skipped']} staged={table['rows_staged']} "
            f"inserted={table['rows_inserted']} updated={table['rows_updated']} "
            f"failed={table['rows_failed']} purged={table.get('rows_purged', 0)}"
        )
        if table.get("purge_candidates") is not None:
            line += f" purge_candidates={table['purge_candidates']}"
        if table.get("purge_error"):
            line += f" purge_error={table['purge_error']}"
        if table.get("error"):
            line += f" error={table['error']}"
        lines.append(line)
    totals = report.get("totals") or {}
    if totals:
        lines.append("")
        lines.append(
            "Totals: "
            + ", ".join(f"{key}={value}" for key, value in totals.items())
        )
    if report.get("verification"):
        lines.append("")
        lines.append(format_verify_report(report["verification"], "text"))
    return "\n".join(lines)


def format_verify_report(report: Mapping[str, Any], output_format: str) -> str:
    if output_format == "json":
        return json.dumps(report, indent=2, sort_keys=True, default=str)
    lines = [
        "WGD verify",
        f"Run: {report.get('run_id')}",
        f"State: {report.get('state')}",
        "",
        "Results:",
    ]
    for result in report.get("results", []):
        line = (
            f"  {result['node']} {result['table']}: {result['verification_state']} "
            f"row_count={result['row_count']} changed_verified={result['changed_row_verification_count']}"
        )
        if result.get("error"):
            line += f" error={result['error']}"
        lines.append(line)
    return "\n".join(lines)


def format_purge_report(report: Mapping[str, Any], output_format: str) -> str:
    if output_format == "json":
        return json.dumps(report, indent=2, sort_keys=True, default=str)
    lines = [
        "WGD purge",
        f"State: {report.get('state')}",
        f"Source run: {report.get('source_run_id')}",
        f"Purge run: {report.get('run_id') or '<none>'}",
        f"Confirmed: {report.get('confirmed')}",
        "",
        "Tables:",
    ]
    for table in report.get("tables", []):
        line = (
            f"  {table['table']}: purge_candidates={table['purge_candidates']} "
            f"purged={table['rows_purged']}"
        )
        if table.get("error"):
            line += f" error={table['error']}"
        lines.append(line)
    totals = report.get("totals") or {}
    if totals:
        lines.append("")
        lines.append("Totals: " + ", ".join(f"{key}={value}" for key, value in totals.items()))
    return "\n".join(lines)


def format_generate_sql_report(report: Mapping[str, Any], output_format: str) -> str:
    if output_format == "json":
        return json.dumps(report, indent=2, sort_keys=True, default=str)
    totals = report.get("totals") or {}
    lines = [
        "WGD generated SQL package",
        f"Output: {report.get('output_dir')}",
        f"State: {report.get('state')}",
        f"Rollback mode: {report.get('rollback_mode')}",
        f"Transaction scope: {report.get('transaction_scope')}",
        f"Sync SQL: {report.get('sync_sql_path')}",
        f"Load staging: {report.get('load_staging_path')}",
        "",
        "Totals: " + ", ".join(f"{key}={value}" for key, value in totals.items()),
    ]
    return "\n".join(lines)


def format_rollback_test_report(report: Mapping[str, Any], output_format: str) -> str:
    if output_format == "json":
        return json.dumps(report, indent=2, sort_keys=True, default=str)
    lines = [
        "WGD rollback test phase",
        f"State: {report.get('state')}",
        f"Execute: {report.get('execute')}",
        f"Output root: {report.get('output_root')}",
        f"Publisher: {report.get('publisher')} database={report.get('database')} schema={report.get('schema')}",
        f"Status DB: {report.get('status_db')} exists={report.get('status_db_exists')}",
        f"Rollback mode: {report.get('rollback_mode')}",
        f"Transaction scope: {report.get('transaction_scope')}",
        "",
        "Phases:",
    ]
    for phase in report.get("phases", []):
        totals = phase.get("totals") or {}
        checks = phase.get("checks") or {}
        lines.append(
            f"  {phase['phase']}: checks={checks.get('state')} "
            f"staged={totals.get('rows_staged', 0)} purge_candidates={totals.get('purge_candidates', 0)} "
            f"output={phase['output_dir']}"
        )
        failed_checks = [check for check in checks.get("checks", []) if not check.get("passed")]
        for check in failed_checks:
            lines.append(f"    failed_check={check['name']}: {check['detail']}")
        commands = phase.get("commands") or {}
        if not report.get("execute"):
            lines.append(f"    working_dir: {commands.get('working_dir')}")
            lines.append(f"    load_staging: {commands.get('load_staging')}")
            lines.append(f"    run_rollback_sql: {commands.get('run_rollback_sql')}")
        execution = phase.get("execution")
        if execution:
            lines.append(
                f"    execution={execution.get('state')} counts_unchanged={execution.get('counts_unchanged')}"
            )
            if execution.get("error"):
                lines.append(f"    error={execution['error']}")
            load_result = execution.get("load_staging") or {}
            if load_result:
                lines.append(f"    load_staging_rc={load_result.get('returncode')}")
            sync_result = execution.get("run_rollback_sql") or {}
            if sync_result:
                lines.append(f"    run_rollback_sql_rc={sync_result.get('returncode')}")
    return "\n".join(lines)


def format_status_report(report: Mapping[str, Any], output_format: str) -> str:
    if output_format == "json":
        return json.dumps(report, indent=2, sort_keys=True, default=str)
    if not report.get("exists"):
        return f"WGD sync status\nStatus DB: {report.get('path')}\nNo status database exists yet."
    latest = report.get("latest_run")
    lines = [f"WGD sync status\nStatus DB: {report.get('path')}"]
    if not latest:
        lines.append("No sync runs recorded yet.")
        return "\n".join(lines)
    lines.extend(
        [
            f"Latest run: {latest['run_id']}",
            f"Mode: {latest['mode']}",
            f"State: {latest['state']}",
            f"Started: {latest['started_at']}",
            f"Finished: {latest.get('finished_at') or '<running>'}",
        ]
    )
    if latest.get("error"):
        lines.append(f"Error: {latest['error']}")
    if report.get("tables"):
        lines.append("")
        lines.append("Tables:")
        for table in report["tables"]:
            lines.append(
                f"  {table['table_name']} ({table['node']}): scanned={table['rows_scanned']} "
                f"skipped={table['rows_skipped']} staged={table['rows_staged']} "
                f"inserted={table['rows_inserted']} updated={table['rows_updated']} "
                f"failed={table['rows_failed']} purged={table.get('rows_purged', 0)}"
            )
    if report.get("source_missing_rows"):
        lines.append("")
        lines.append("Rows absent from latest source scan:")
        for table in report["source_missing_rows"]:
            lines.append(f"  {table['table_name']}: {table['missing_rows']}")
    if report.get("purged_rows"):
        lines.append("")
        lines.append("Rows purged by latest run:")
        for table in report["purged_rows"]:
            lines.append(f"  {table['table_name']}: {table['purged_rows']}")
    return "\n".join(lines)


def quote_sqlite_identifier(name: str) -> str:
    if "\x00" in name:
        raise SyncError("SQLite identifiers cannot contain NUL bytes")
    return '"' + name.replace('"', '""') + '"'


def quote_sql_identifier(name: str) -> str:
    if not name or "\x00" in name:
        raise SyncError("SQL identifiers cannot be empty or contain NUL bytes")
    return "[" + name.replace("]", "]]") + "]"


def qualified_sql_name(schema: str, table: str) -> str:
    return f"{quote_sql_identifier(schema)}.{quote_sql_identifier(table)}"


def chunked(values: Sequence[Any], size: int) -> Iterator[Sequence[Any]]:
    for index in range(0, len(values), size):
        yield values[index : index + size]


def commit_connection(connection: Any) -> None:
    try:
        connection.commit()
    except Exception:
        pass


def rollback_connection(connection: Any) -> None:
    try:
        connection.rollback()
    except Exception:
        pass


def close_connection(connection: Any) -> None:
    try:
        connection.close()
    except Exception:
        pass


if __name__ == "__main__":
    raise SystemExit(main())
