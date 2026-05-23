#!/usr/bin/env python3
"""Read-only smoke query for the WGD graph/vector tables in SQL Server."""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any, Iterable


ROOT = Path(__file__).resolve().parents[2]
MSSQLT_DIR = ROOT / "mssqlt"
if str(MSSQLT_DIR) not in sys.path:
    sys.path.insert(0, str(MSSQLT_DIR))

from mssqltop import (  # noqa: E402
    ConfigError,
    DependencyError,
    connect,
    import_pyodbc,
    load_config,
    read_password,
)


DEFAULT_CONFIG_PATH = Path(__file__).with_name("config.ini")
DEFAULT_SCHEMA = "dbo"
DEFAULT_SAMPLE_SIZE = 5
PREFERRED_PREFIXES = ("tmp_", "")
EXPECTED_TABLES = (
    "spatial_nodes",
    "spatial_edges",
    "spatial_cells",
    "cell_vectors",
    "graph_clusters",
    "graph_cluster_members",
    "cluster_vectors",
)
SAMPLE_COLUMNS = {
    "spatial_nodes": (
        "node_key",
        "node_type",
        "canonical_scene_id",
        "label",
        "source_table",
        "source_key",
        "vector_dim",
    ),
    "cell_vectors": (
        "cell_key",
        "modality",
        "vector_model",
        "vector_dim",
        "member_count",
    ),
    "cluster_vectors": (
        "cluster_key",
        "modality",
        "vector_model",
        "vector_dim",
        "member_count",
    ),
}
IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
PREFIX_RE = re.compile(r"^[A-Za-z0-9_]*$")


class QueryError(RuntimeError):
    """Raised when the graph/vector query cannot be completed."""


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Query WGD graph/vector tables in Microsoft SQL Server.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG_PATH,
        help=f"mssqltop-style config path (default: {DEFAULT_CONFIG_PATH})",
    )
    parser.add_argument(
        "--schema",
        default=DEFAULT_SCHEMA,
        help=f"SQL Server schema containing WGD tables (default: {DEFAULT_SCHEMA})",
    )
    parser.add_argument(
        "--table-prefix",
        help="table prefix to use, such as tmp_; omit to prefer tmp_ and fall back to unprefixed tables",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="output format (default: text)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        report = query_with_config(args)
    except (ConfigError, DependencyError, QueryError) as exc:
        print(f"wgd-query: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:
        print(f"wgd-query: {friendly_error(exc)}", file=sys.stderr)
        return 1

    print(format_report(report, args.format))
    return 0


def query_with_config(args: argparse.Namespace) -> dict[str, Any]:
    schema = validate_schema(args.schema)
    table_prefix = validate_prefix(args.table_prefix) if args.table_prefix is not None else None

    config = load_config(str(args.config))
    password = read_password(config)
    pyodbc = import_pyodbc()
    connection = connect(config, password, pyodbc)
    try:
        return collect_report(
            connection,
            schema=schema,
            requested_prefix=table_prefix,
            sample_size=DEFAULT_SAMPLE_SIZE,
        )
    finally:
        close_connection(connection)


def collect_report(
    connection: Any,
    *,
    schema: str,
    requested_prefix: str | None,
    sample_size: int = DEFAULT_SAMPLE_SIZE,
) -> dict[str, Any]:
    prefix = detect_table_prefix(connection, schema, requested_prefix)
    table_names = [prefix + table for table in EXPECTED_TABLES]
    counts = count_tables(connection, schema, table_names)
    samples = {
        table: sample_table(connection, schema, prefix + table, table, sample_size)
        for table in SAMPLE_COLUMNS
    }
    return {
        "schema": schema,
        "table_prefix": prefix,
        "table_counts": [
            {
                "table": base_name,
                "physical_table": prefix + base_name,
                "row_count": counts[prefix + base_name],
            }
            for base_name in EXPECTED_TABLES
        ],
        "samples": samples,
    }


def detect_table_prefix(connection: Any, schema: str, requested_prefix: str | None) -> str:
    if requested_prefix is not None:
        missing = missing_expected_tables(connection, schema, requested_prefix)
        if missing:
            raise QueryError(
                "missing expected WGD table(s) for prefix "
                f"{requested_prefix!r}: {', '.join(missing)}"
            )
        return requested_prefix

    missing_by_prefix: dict[str, list[str]] = {}
    for prefix in PREFERRED_PREFIXES:
        missing = missing_expected_tables(connection, schema, prefix)
        if not missing:
            return prefix
        missing_by_prefix[prefix] = missing

    parts = [
        f"{prefix or '<none>'}: {', '.join(missing)}"
        for prefix, missing in missing_by_prefix.items()
    ]
    raise QueryError("could not find a complete WGD table set; missing by prefix: " + "; ".join(parts))


def missing_expected_tables(connection: Any, schema: str, prefix: str) -> list[str]:
    expected = [prefix + table for table in EXPECTED_TABLES]
    present = table_names(connection, schema, expected)
    return [table for table in expected if table not in present]


def table_names(connection: Any, schema: str, names: Iterable[str]) -> set[str]:
    names = list(names)
    placeholders = ", ".join("?" for _ in names)
    sql = f"""
SELECT t.name
FROM sys.tables AS t
JOIN sys.schemas AS s ON s.schema_id = t.schema_id
WHERE s.name = ? AND t.name IN ({placeholders})
"""
    cursor = connection.cursor()
    try:
        cursor.execute(sql, schema, *names)
        return {str(row[0]) for row in cursor.fetchall()}
    finally:
        cursor.close()


def count_tables(connection: Any, schema: str, table_names: Iterable[str]) -> dict[str, int]:
    return {
        table_name: scalar_int(
            connection,
            f"SELECT COUNT_BIG(*) AS row_count FROM {qualified_name(schema, table_name)}",
        )
        for table_name in table_names
    }


def sample_table(
    connection: Any,
    schema: str,
    physical_table: str,
    logical_table: str,
    sample_size: int,
) -> list[dict[str, Any]]:
    columns = SAMPLE_COLUMNS[logical_table]
    select_columns = ", ".join(quote_identifier(column) for column in columns)
    order_column = quote_identifier(columns[0])
    sql = f"""
SELECT TOP ({sample_size})
    {select_columns},
    DATALENGTH([vector]) AS [vector_bytes]
FROM {qualified_name(schema, physical_table)}
ORDER BY {order_column}
"""
    cursor = connection.cursor()
    try:
        cursor.execute(sql)
        names = [str(column[0]) for column in cursor.description]
        return [dict(zip(names, row)) for row in cursor.fetchall()]
    finally:
        cursor.close()


def scalar_int(connection: Any, sql: str) -> int:
    cursor = connection.cursor()
    try:
        cursor.execute(sql)
        row = cursor.fetchone()
        if row is None:
            raise QueryError("count query returned no rows")
        return int(row[0])
    finally:
        cursor.close()


def format_report(report: dict[str, Any], output_format: str) -> str:
    if output_format == "json":
        return json.dumps(report, indent=2, sort_keys=True, default=str)
    return format_text_report(report)


def format_text_report(report: dict[str, Any]) -> str:
    lines = [
        "WGD Graph Vector Database",
        f"Schema: {report['schema']}",
        f"Table prefix: {report['table_prefix'] or '<none>'}",
        "",
        "Row counts:",
    ]
    for table in report["table_counts"]:
        lines.append(f"  {table['physical_table']}: {table['row_count']}")

    lines.append("")
    lines.append("Samples:")
    for table_name, rows in report["samples"].items():
        lines.append(f"  {table_name}:")
        if not rows:
            lines.append("    <no rows>")
            continue
        for row in rows:
            fields = ", ".join(f"{key}={compact_value(value)}" for key, value in row.items())
            lines.append(f"    {fields}")
    return "\n".join(lines)


def compact_value(value: Any) -> str:
    if value is None:
        return "NULL"
    text = str(value).replace("\r", " ").replace("\n", " ")
    if len(text) > 80:
        return text[:77] + "..."
    return text


def validate_schema(schema: str) -> str:
    if not IDENTIFIER_RE.match(schema):
        raise QueryError(f"unsafe SQL schema identifier: {schema!r}")
    return schema


def validate_prefix(prefix: str) -> str:
    if not PREFIX_RE.match(prefix):
        raise QueryError(f"unsafe SQL table prefix: {prefix!r}")
    return prefix


def qualified_name(schema: str, table_name: str) -> str:
    return f"{quote_identifier(schema)}.{quote_identifier(table_name)}"


def quote_identifier(name: str) -> str:
    if not PREFIX_RE.match(name):
        raise QueryError(f"unsafe SQL identifier: {name!r}")
    return f"[{name}]"


def close_connection(connection: Any) -> None:
    try:
        connection.close()
    except Exception:
        pass


def friendly_error(exc: Exception) -> str:
    detail = str(exc).strip()
    return detail or exc.__class__.__name__


if __name__ == "__main__":
    raise SystemExit(main())
