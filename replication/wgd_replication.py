#!/usr/bin/env python3
"""Operator helpers for WGD lab replication across SQL Server nodes."""

from __future__ import annotations

import argparse
from configparser import ConfigParser
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
from typing import Any, Iterable


DEFAULT_CONFIG = Path(__file__).with_name("nodes.ini.example")
DEFAULT_DRIVER = "ODBC Driver 18 for SQL Server"
DEFAULT_PASSWORD_FILE = Path("/tmp/mssqlpass")
WGD_TABLES = (
    "tmp_cameras",
    "tmp_cell_vectors",
    "tmp_cluster_vectors",
    "tmp_descriptions",
    "tmp_graph_cluster_members",
    "tmp_graph_clusters",
    "tmp_lights",
    "tmp_materials",
    "tmp_metadata",
    "tmp_objects",
    "tmp_povs",
    "tmp_scenes",
    "tmp_source_chunks",
    "tmp_source_edges",
    "tmp_source_files",
    "tmp_source_symbols",
    "tmp_spatial_cells",
    "tmp_spatial_edges",
    "tmp_spatial_nodes",
)


@dataclass(frozen=True)
class Node:
    name: str
    server: str
    role: str
    expected_sql_name: str
    replication_name: str


@dataclass(frozen=True)
class ReplConfig:
    database: str
    schema: str
    publisher: str
    publication: str
    password_file: Path
    nodes: dict[str, Node]


class ReplError(RuntimeError):
    """Raised when replication tooling cannot continue."""


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Manage WGD SQL Server lab replication.")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG, help=f"node config path (default: {DEFAULT_CONFIG})")
    subparsers = parser.add_subparsers(dest="command", required=True)

    preflight = subparsers.add_parser("preflight", help="inspect all nodes without changing them")
    preflight.add_argument("--format", choices=("text", "json"), default="text")

    backup = subparsers.add_parser("backup", help="create copy-only backups on each reachable node")
    backup.add_argument("--node", action="append", help="node name to back up; may be repeated")
    backup.add_argument("--no-compression", action="store_true", help="do not request SQL Server backup compression")

    create = subparsers.add_parser("create", help="print NODE010 replication create SQL")
    create.add_argument("--password", required=True, help="SQL sa password embedded into the generated SQL")
    create.add_argument("--skip-guards", action="store_true", help="omit generated safety guards")

    recreate = subparsers.add_parser("recreate", help="print SQL to drop and recreate NODE010 replication metadata")
    recreate.add_argument("--password", required=True, help="SQL sa password embedded into the generated SQL")
    recreate.add_argument("--skip-guards", action="store_true", help="omit generated safety guards")

    agent_fix = subparsers.add_parser("generate-agent-fix-sql", help="print SQL to repair existing replication agent jobs")
    agent_fix.add_argument("--password", help="SQL sa password embedded into the generated SQL")
    agent_fix.add_argument(
        "--password-file",
        type=Path,
        help="read SQL sa password from this file when --password is omitted (default: config password_file)",
    )

    alias_fix = subparsers.add_parser(
        "generate-client-alias-fix",
        help="print Windows SQL client alias commands for the NODE010 SQL Agent host",
    )
    alias_fix.add_argument("--shell", choices=("powershell", "cmd", "wsl"), default="powershell")

    rename = subparsers.add_parser("generate-name-fix-sql", help="print SQL to repair cloned @@SERVERNAME values")
    rename.add_argument("--node", action="append", help="node name to include; may be repeated")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        config = load_repl_config(args.config)
        if args.command == "preflight":
            report = preflight(config)
            print(json.dumps(report, indent=2, default=str) if args.format == "json" else format_preflight(report))
        elif args.command == "backup":
            nodes = args.node or sorted(config.nodes)
            results = backup_nodes(config, nodes, compression=not args.no_compression)
            print(format_backup_results(results))
        elif args.command == "create":
            print(generate_create_replication_sql(config, args.password, skip_guards=args.skip_guards))
        elif args.command == "recreate":
            print(generate_recreate_replication_sql(config, args.password, skip_guards=args.skip_guards))
        elif args.command == "generate-agent-fix-sql":
            password = args.password if args.password is not None else read_password(args.password_file or config.password_file)
            print(generate_agent_command_sql(config, password))
        elif args.command == "generate-client-alias-fix":
            print(generate_client_alias_fix(config, shell=args.shell))
        elif args.command == "generate-name-fix-sql":
            print(generate_name_fix_sql(config, args.node))
        else:
            raise ReplError(f"unknown command: {args.command}")
    except ReplError as exc:
        print(f"wgd-replication: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:
        print(f"wgd-replication: {exc}", file=sys.stderr)
        return 1
    return 0


def load_repl_config(path: Path) -> ReplConfig:
    parser = ConfigParser(interpolation=None)
    if not parser.read(path):
        raise ReplError(f"cannot read config: {path}")
    if not parser.has_section("replication"):
        raise ReplError(f"config {path} is missing [replication]")
    repl = parser["replication"]
    nodes: dict[str, Node] = {}
    for section in parser.sections():
        if section == "replication":
            continue
        table = parser[section]
        nodes[section] = Node(
            name=section,
            server=required(table, "server", section),
            role=required(table, "role", section),
            expected_sql_name=table.get("expected_sql_name", section).strip() or section,
            replication_name=table.get(
                "replication_name",
                replication_name_for(table.get("expected_sql_name", section).strip() or section, required(table, "server", section)),
            ).strip(),
        )
    publisher = repl.get("publisher", "NODE010").strip()
    if publisher not in nodes:
        raise ReplError(f"publisher {publisher!r} is not a configured node")
    return ReplConfig(
        database=repl.get("database", "POVIID").strip(),
        schema=repl.get("schema", "dbo").strip(),
        publisher=publisher,
        publication=repl.get("publication", "WGD_TMP_Publication").strip(),
        password_file=Path(repl.get("password_file", str(DEFAULT_PASSWORD_FILE)).strip()),
        nodes=nodes,
    )


def required(table: Any, key: str, section: str) -> str:
    value = table.get(key)
    if value is None or not value.strip():
        raise ReplError(f"config section [{section}] is missing {key}")
    return value.strip()


def replication_name_for(sql_name: str, endpoint: str) -> str:
    port = endpoint_port(endpoint)
    if port and port != "1433":
        return f"{sql_name},{port}"
    return sql_name


def endpoint_port(endpoint: str) -> str | None:
    if "," not in endpoint:
        return None
    tail = endpoint.rsplit(",", 1)[1].strip()
    return tail if tail.isdigit() else None


def agent_endpoint_for(node: Node) -> str:
    endpoint = node.server.strip()
    if ":" in endpoint.split(",", 1)[0]:
        return endpoint
    return f"tcp:{endpoint}"


def read_password(path: Path) -> str:
    try:
        password = path.read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise ReplError(f"cannot read password file {path}: {exc}") from exc
    if not password:
        raise ReplError(f"password file is empty: {path}")
    return password


def import_pyodbc() -> Any:
    try:
        import pyodbc  # type: ignore
    except ImportError as exc:
        raise ReplError("pyodbc is required for live node operations") from exc
    return pyodbc


def connect(node: Node, config: ReplConfig, *, database: str | None = None, timeout: int = 10) -> Any:
    password = read_password(config.password_file)
    pyodbc = import_pyodbc()
    parts = [
        f"DRIVER={{{DEFAULT_DRIVER}}}",
        f"SERVER={node.server}",
        f"DATABASE={database or 'master'}",
        "UID=sa",
        f"PWD={password}",
        "Pooling=False",
        "Encrypt=no",
        "TrustServerCertificate=yes",
        f"Connection Timeout={timeout}",
        "Application Name=WGD replication tooling",
    ]
    return pyodbc.connect(";".join(parts), autocommit=True, timeout=timeout)


def preflight(config: ReplConfig) -> dict[str, Any]:
    nodes = []
    for node in config.nodes.values():
        entry: dict[str, Any] = {
            "node": node.name,
            "server": node.server,
            "role": node.role,
            "expected_sql_name": node.expected_sql_name,
            "replication_name": node.replication_name,
        }
        try:
            connection = connect(node, config)
        except Exception as exc:
            entry["connect_ok"] = False
            entry["error"] = str(exc)
            nodes.append(entry)
            continue
        try:
            entry["connect_ok"] = True
            entry.update(server_properties(connection))
            entry.update(database_properties(connection, config.database))
            entry["table_counts"] = table_counts(connection, config)
            entry["primary_keys"] = primary_keys(connection, config)
            entry["services"] = services(connection)
        finally:
            connection.close()
        nodes.append(entry)
    return {"database": config.database, "publication": config.publication, "nodes": nodes, "blockers": blockers(config, nodes)}


def server_properties(connection: Any) -> dict[str, Any]:
    row = one_row(
        connection,
        """
        SELECT
          @@SERVERNAME AS sql_name,
          CAST(SERVERPROPERTY('MachineName') AS nvarchar(256)) AS machine_name,
          CAST(SERVERPROPERTY('ServerName') AS nvarchar(256)) AS server_name,
          CAST(SERVERPROPERTY('Edition') AS nvarchar(256)) AS edition,
          CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(256)) AS product_version,
          CAST(SERVERPROPERTY('InstanceDefaultBackupPath') AS nvarchar(4000)) AS backup_path
        """,
    )
    return dict(row)


def database_properties(connection: Any, database: str) -> dict[str, Any]:
    row = one_row(
        connection,
        """
        SELECT d.name AS database_name, d.state_desc, d.recovery_model_desc,
               CAST(SUM(f.size) * 8.0 / 1024.0 AS decimal(18,1)) AS size_mb,
               d.is_published, d.is_subscribed, d.is_distributor
        FROM sys.databases AS d
        JOIN sys.master_files AS f ON f.database_id = d.database_id
        WHERE d.name = ?
        GROUP BY d.name, d.state_desc, d.recovery_model_desc, d.is_published, d.is_subscribed, d.is_distributor
        """,
        database,
    )
    return {"database": dict(row)}


def table_counts(connection: Any, config: ReplConfig) -> dict[str, int]:
    placeholders = ", ".join("?" for _ in WGD_TABLES)
    rows = all_rows(
        connection,
        f"""
        SELECT t.name, SUM(CASE WHEN p.index_id IN (0,1) THEN p.row_count ELSE 0 END) AS row_count
        FROM [{config.database}].sys.tables AS t
        JOIN [{config.database}].sys.schemas AS s ON s.schema_id = t.schema_id
        JOIN [{config.database}].sys.dm_db_partition_stats AS p ON p.object_id = t.object_id
        WHERE s.name = ? AND t.name IN ({placeholders})
        GROUP BY t.name
        """,
        config.schema,
        *WGD_TABLES,
    )
    return {str(row["name"]): int(row["row_count"]) for row in rows}


def primary_keys(connection: Any, config: ReplConfig) -> dict[str, str]:
    placeholders = ", ".join("?" for _ in WGD_TABLES)
    rows = all_rows(
        connection,
        f"""
        SELECT t.name AS table_name,
               STRING_AGG(c.name, ',') WITHIN GROUP (ORDER BY ic.key_ordinal) AS pk_columns
        FROM [{config.database}].sys.tables AS t
        JOIN [{config.database}].sys.schemas AS s ON s.schema_id = t.schema_id
        LEFT JOIN [{config.database}].sys.key_constraints AS kc ON kc.parent_object_id = t.object_id AND kc.type = 'PK'
        LEFT JOIN [{config.database}].sys.index_columns AS ic ON ic.object_id = t.object_id AND ic.index_id = kc.unique_index_id
        LEFT JOIN [{config.database}].sys.columns AS c ON c.object_id = t.object_id AND c.column_id = ic.column_id
        WHERE s.name = ? AND t.name IN ({placeholders})
        GROUP BY t.name
        """,
        config.schema,
        *WGD_TABLES,
    )
    return {str(row["table_name"]): str(row["pk_columns"] or "") for row in rows}


def services(connection: Any) -> list[dict[str, Any]]:
    try:
        return [dict(row) for row in all_rows(connection, "SELECT servicename, startup_type_desc, status_desc FROM sys.dm_server_services ORDER BY servicename")]
    except Exception:
        return []


def blockers(config: ReplConfig, nodes: list[dict[str, Any]]) -> list[str]:
    issues: list[str] = []
    connected = [node for node in nodes if node.get("connect_ok")]
    for node in nodes:
        if not node.get("connect_ok"):
            issues.append(f"{node['node']} is not reachable: {node.get('error')}")
    for node in connected:
        config_node = config.nodes[str(node["node"])]
        if node.get("sql_name") != config_node.expected_sql_name:
            issues.append(
                f"{node['node']} config endpoint {config_node.server} reached SQL Server "
                f"{node.get('sql_name')!r}, expected {config_node.expected_sql_name!r}"
            )
    sql_names = [str(node.get("sql_name")) for node in connected if node.get("sql_name")]
    duplicates = sorted({name for name in sql_names if sql_names.count(name) > 1})
    for name in duplicates:
        issues.append(f"duplicate SQL Server logical name detected: {name}")
    publisher = next((node for node in connected if node["node"] == config.publisher), None)
    if not publisher:
        issues.append(f"publisher {config.publisher} is not reachable")
    elif "Developer" not in str(publisher.get("edition")):
        issues.append(f"publisher {config.publisher} is not Developer Edition")
    elif not any(service.get("servicename", "").startswith("SQL Server Agent") and service.get("status_desc") == "Running" for service in publisher.get("services", [])):
        issues.append(f"publisher {config.publisher} SQL Server Agent is not running")
    for node in connected:
        missing_tables = sorted(set(WGD_TABLES) - set(node.get("table_counts", {})))
        if missing_tables:
            issues.append(f"{node['node']} is missing table(s): {', '.join(missing_tables)}")
        missing_pk = [table for table, columns in node.get("primary_keys", {}).items() if not columns]
        if missing_pk:
            issues.append(f"{node['node']} has table(s) without primary keys: {', '.join(sorted(missing_pk))}")
    return issues


def backup_nodes(config: ReplConfig, node_names: Iterable[str], *, compression: bool) -> list[dict[str, Any]]:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    results = []
    for node_name in node_names:
        node = config.nodes.get(node_name)
        if node is None:
            raise ReplError(f"unknown node: {node_name}")
        result: dict[str, Any] = {"node": node.name, "server": node.server}
        try:
            connection = connect(node, config, timeout=10)
            connection.timeout = 0
            try:
                props = server_properties(connection)
                backup_dir = str(props.get("backup_path") or "/var/opt/mssql/data").rstrip("/\\")
                slash = "\\" if "\\" in backup_dir else "/"
                backup_path = f"{backup_dir}{slash}{config.database}_{node.name}_{timestamp}.bak"
                sql = backup_sql(config.database, backup_path, compression=compression)
                cursor = connection.cursor()
                cursor.execute(sql)
                cursor.close()
                result["ok"] = True
                result["backup_path"] = backup_path
            finally:
                connection.close()
        except Exception as exc:
            result["ok"] = False
            result["error"] = str(exc)
        results.append(result)
    return results


def backup_sql(database: str, backup_path: str, *, compression: bool) -> str:
    options = ["COPY_ONLY", "INIT", "CHECKSUM", "STATS = 10"]
    if compression:
        options.insert(2, "COMPRESSION")
    escaped_path = backup_path.replace("'", "''")
    return f"BACKUP DATABASE {q(database)} TO DISK = N'{escaped_path}' WITH {', '.join(options)};"


def generate_create_replication_sql(config: ReplConfig, password: str, *, skip_guards: bool) -> str:
    publisher = config.nodes[config.publisher]
    subscribers = [node for node in config.nodes.values() if node.role == "subscriber"]
    publisher_name = publisher.replication_name
    lines = [
        "-- WGD lab transactional replication setup.",
        "-- Run on NODE010 after SQL Server logical names and connectivity are fixed.",
        "-- The supplied SQL password is embedded in replication agent commands.",
        "USE [master];",
        "GO",
    ]
    if not skip_guards:
        lines.extend(
            [
                f"IF @@SERVERNAME <> N'{publisher.expected_sql_name}'",
                f"    THROW 51000, 'Run this script on logical publisher {publisher.expected_sql_name}. Fix @@SERVERNAME first.', 1;",
                "GO",
                "IF CAST(SERVERPROPERTY('Edition') AS nvarchar(256)) NOT LIKE '%Developer%'",
                "    THROW 51001, 'NODE010 must be Developer Edition for lab publishing.', 1;",
                "GO",
            ]
        )
    lines.extend(
        [
            f"EXEC sys.sp_adddistributor @distributor = {tsql_string(publisher_name)};",
            "GO",
            "IF DB_ID(N'distribution') IS NULL",
            "BEGIN",
            f"    EXEC sys.sp_adddistributiondb @database = N'distribution', @security_mode = 0, @login = N'sa', @password = {tsql_string(password)};",
            "END",
            "GO",
            f"EXEC sys.sp_adddistpublisher @publisher = {tsql_string(publisher_name)}, @distribution_db = N'distribution', @security_mode = 1;",
            "GO",
            f"USE {q(config.database)};",
            "GO",
            f"EXEC sys.sp_replicationdboption @dbname = N'{config.database}', @optname = N'publish', @value = N'true';",
            "GO",
            f"IF NOT EXISTS (SELECT 1 FROM syspublications WHERE name = N'{config.publication}')",
            "BEGIN",
            f"    EXEC sys.sp_addpublication @publication = N'{config.publication}', @description = N'WGD tmp table publication', @sync_method = N'native', @retention = 168, @allow_push = N'true', @allow_pull = N'false', @allow_anonymous = N'false', @enabled_for_internet = N'false', @repl_freq = N'continuous', @status = N'active', @independent_agent = N'true', @immediate_sync = N'false', @replicate_ddl = 1;",
            "END",
            "GO",
            f"EXEC sys.sp_addpublication_snapshot @publication = N'{config.publication}', @frequency_type = 1, @publisher_security_mode = 1;",
            "GO",
            f"EXEC sys.sp_changelogreader_agent @publisher_security_mode = 0, @publisher_login = N'sa', @publisher_password = {tsql_string(password)};",
            "GO",
        ]
    )
    for table in WGD_TABLES:
        article = table
        lines.extend(
            [
                f"IF NOT EXISTS (SELECT 1 FROM sysarticles WHERE name = N'{article}')",
                "BEGIN",
                f"    EXEC sys.sp_addarticle @publication = N'{config.publication}', @article = N'{article}', @source_owner = N'{config.schema}', @source_object = N'{table}', @type = N'logbased', @description = NULL, @creation_script = NULL, @pre_creation_cmd = N'none', @schema_option = 0x000000000803509F, @identityrangemanagementoption = N'manual', @destination_table = N'{table}', @destination_owner = N'{config.schema}';",
                "END",
                "GO",
            ]
        )
    lines.extend(
        [
            "-- Push subscribers use SQL Authentication so Linux SQL Agent jobs do not need SSPI.",
        ]
    )
    for offset, subscriber in enumerate(subscribers):
        start_time = 10000 + (offset * 3000)
        subscriber_name = subscriber.replication_name
        lines.extend(
            [
                f"EXEC sys.sp_addsubscription @publication = N'{config.publication}', @subscriber = {tsql_string(subscriber_name)}, @destination_db = N'{config.database}', @subscription_type = N'Push', @sync_type = N'replication support only', @article = N'all', @update_mode = N'read only', @subscriber_type = 0;",
                "GO",
                f"EXEC sys.sp_addpushsubscription_agent @publication = N'{config.publication}', @subscriber = {tsql_string(subscriber_name)}, @subscriber_db = N'{config.database}', @subscriber_security_mode = 0, @subscriber_login = N'sa', @subscriber_password = {tsql_string(password)}, @frequency_type = 4, @frequency_interval = 1, @frequency_subday = 4, @frequency_subday_interval = 60, @active_start_time_of_day = {start_time}, @active_end_time_of_day = 235959;",
                "GO",
            ]
        )
    lines.extend(["-- Normalize generated SQL Agent commands.", generate_agent_command_sql(config, password)])
    return "\n".join(lines)


def generate_recreate_replication_sql(config: ReplConfig, password: str, *, skip_guards: bool) -> str:
    publisher = config.nodes[config.publisher]
    publisher_name = publisher.replication_name
    subscribers = [node for node in config.nodes.values() if node.role == "subscriber"]
    known_subscriber_names = sorted({node.expected_sql_name for node in subscribers} | {node.replication_name for node in subscribers})
    lines = [
        "-- WGD lab transactional replication metadata recreate.",
        "-- This script removes and recreates replication metadata/jobs only.",
        "-- It leaves all WGD data tables and rows in place.",
        "USE [master];",
        "GO",
    ]
    if not skip_guards:
        lines.extend(
            [
                f"IF @@SERVERNAME <> N'{publisher.expected_sql_name}'",
                f"    THROW 52000, 'Run this rebuild script on logical publisher {publisher.expected_sql_name}.', 1;",
                "GO",
                "IF CAST(SERVERPROPERTY('Edition') AS nvarchar(256)) NOT LIKE '%Developer%'",
                "    THROW 52001, 'NODE010 must be Developer Edition for lab publishing.', 1;",
                "GO",
            ]
        )
    lines.extend(
        [
            "-- Stop current replication jobs if they exist. Ignore already-stopped jobs.",
            "DECLARE @JobName sysname;",
            "DECLARE job_cursor CURSOR LOCAL FAST_FORWARD FOR",
            "SELECT name",
            "FROM msdb.dbo.sysjobs",
            f"WHERE name LIKE {tsql_string(publisher.expected_sql_name + '-' + config.database + '-%')};",
            "OPEN job_cursor;",
            "FETCH NEXT FROM job_cursor INTO @JobName;",
            "WHILE @@FETCH_STATUS = 0",
            "BEGIN",
            "    BEGIN TRY",
            "        EXEC msdb.dbo.sp_stop_job @job_name = @JobName;",
            "    END TRY",
            "    BEGIN CATCH",
            "        PRINT CONCAT('stop skipped for ', @JobName, ': ', ERROR_MESSAGE());",
            "    END CATCH;",
            "    FETCH NEXT FROM job_cursor INTO @JobName;",
            "END",
            "CLOSE job_cursor;",
            "DEALLOCATE job_cursor;",
            "GO",
            f"USE {q(config.database)};",
            "GO",
            f"IF EXISTS (SELECT 1 FROM syspublications WHERE name = N'{config.publication}')",
            "BEGIN",
            f"    EXEC sys.sp_dropsubscription @publication = N'{config.publication}', @article = N'all', @subscriber = N'all', @destination_db = N'all';",
            f"    EXEC sys.sp_droppublication @publication = N'{config.publication}';",
            "END",
            "GO",
            f"IF EXISTS (SELECT 1 FROM sys.databases WHERE name = N'{config.database}' AND is_published = 1)",
            f"    EXEC sys.sp_replicationdboption @dbname = N'{config.database}', @optname = N'publish', @value = N'false';",
            "GO",
            "USE [master];",
            "GO",
            f"EXEC sys.sp_removedbreplication @dbname = N'{config.database}';",
            "GO",
        ]
    )
    for subscriber in subscribers:
        lines.extend(
            [
                "BEGIN TRY",
                f"    EXEC sys.sp_dropsubscriber @subscriber = N'{subscriber.expected_sql_name}', @reserved = N'drop_subscriptions';",
                "END TRY",
                "BEGIN CATCH",
                f"    PRINT CONCAT('subscriber cleanup skipped for {subscriber.expected_sql_name}: ', ERROR_MESSAGE());",
                "END CATCH;",
                "GO",
            ]
        )
    for subscriber_name in known_subscriber_names:
        lines.extend(
            [
                f"IF EXISTS (SELECT 1 FROM sys.servers WHERE name = N'{subscriber_name}' AND server_id <> 0)",
                f"    EXEC sys.sp_dropserver @server = N'{subscriber_name}', @droplogins = 'droplogins';",
                "GO",
            ]
        )
    lines.extend(
        [
            "IF DB_ID(N'distribution') IS NOT NULL",
            "BEGIN",
            "    BEGIN TRY",
            f"        EXEC sys.sp_dropdistpublisher @publisher = N'{publisher.expected_sql_name}', @no_checks = 1;",
            "    END TRY",
            "    BEGIN CATCH",
            f"        PRINT CONCAT('distpublisher cleanup skipped for {publisher.expected_sql_name}: ', ERROR_MESSAGE());",
            "    END CATCH;",
            "END",
            "GO",
        ]
    )
    for distributor_name in [publisher.expected_sql_name, "repl_distributor"]:
        lines.extend(
            [
                f"IF EXISTS (SELECT 1 FROM sys.servers WHERE name = N'{distributor_name}' AND server_id <> 0)",
                f"    EXEC sys.sp_dropserver @server = N'{distributor_name}', @droplogins = 'droplogins';",
                "GO",
            ]
        )
    lines.extend(
        [
            "IF DB_ID(N'distribution') IS NOT NULL",
            "BEGIN",
            "    BEGIN TRY",
            "        EXEC sys.sp_dropdistributiondb @database = N'distribution';",
            "    END TRY",
            "    BEGIN CATCH",
            "        PRINT CONCAT('distribution db cleanup skipped: ', ERROR_MESSAGE());",
            "    END CATCH;",
            "END",
            "GO",
            "BEGIN TRY",
            "    EXEC sys.sp_dropdistributor @no_checks = 1, @ignore_distributor = 1;",
            "END TRY",
            "BEGIN CATCH",
            "    PRINT CONCAT('distributor cleanup skipped: ', ERROR_MESSAGE());",
            "END CATCH;",
            "GO",
            f"PRINT 'Rebuilding WGD replication using publisher/distributor {publisher_name}';",
            "GO",
            generate_create_replication_sql(config, password, skip_guards=skip_guards),
        ]
    )
    return "\n".join(lines)


def generate_agent_command_sql(config: ReplConfig, password: str) -> str:
    publisher = config.nodes[config.publisher]
    subscribers = [node for node in config.nodes.values() if node.role == "subscriber"]
    password_value = "[" + tsql_bracket_value(password) + "]"
    publisher_endpoint = agent_endpoint_for(publisher)
    job_prefix = f"{publisher.expected_sql_name}-{config.database}-%"
    subscriber_endpoint_checks = [
        line
        for subscriber in subscribers
        for line in (
            f"    IF CHARINDEX(N'-Subscriber [{subscriber.expected_sql_name}]', @NewCommand) > 0 SET @SubscriberName = N'{subscriber.expected_sql_name}';",
            f"    IF CHARINDEX(N'-Subscriber [{subscriber.expected_sql_name.lower()}]', @NewCommand) > 0 SET @SubscriberName = N'{subscriber.expected_sql_name}';",
            f"    IF CHARINDEX(N'-Subscriber [{subscriber.replication_name}]', @NewCommand) > 0 SET @SubscriberName = N'{subscriber.expected_sql_name}';",
            f"    IF CHARINDEX(N'-Subscriber [{subscriber.replication_name.lower()}]', @NewCommand) > 0 SET @SubscriberName = N'{subscriber.expected_sql_name}';",
            f"    IF CHARINDEX(N'-Subscriber [{agent_endpoint_for(subscriber)}]', @NewCommand) > 0 SET @SubscriberName = N'{subscriber.expected_sql_name}';",
            f"    IF CHARINDEX(N'-Subscriber [{agent_endpoint_for(subscriber).lower()}]', @NewCommand) > 0 SET @SubscriberName = N'{subscriber.expected_sql_name}';",
        )
    ]
    subscriber_replacements = [
        line
        for subscriber in subscribers
        for line in (
            f"            SET @NewCommand = REPLACE(@NewCommand, N'-Subscriber [{subscriber.expected_sql_name}]', N'-Subscriber [' + @SubscriberName + N']');",
            f"            SET @NewCommand = REPLACE(@NewCommand, N'-Subscriber [{subscriber.expected_sql_name.lower()}]', N'-Subscriber [' + @SubscriberName + N']');",
            f"            SET @NewCommand = REPLACE(@NewCommand, N'-Subscriber [{subscriber.replication_name}]', N'-Subscriber [' + @SubscriberName + N']');",
            f"            SET @NewCommand = REPLACE(@NewCommand, N'-Subscriber [{subscriber.replication_name.lower()}]', N'-Subscriber [' + @SubscriberName + N']');",
            f"            SET @NewCommand = REPLACE(@NewCommand, N'-Subscriber [{agent_endpoint_for(subscriber)}]', N'-Subscriber [' + @SubscriberName + N']');",
            f"            SET @NewCommand = REPLACE(@NewCommand, N'-Subscriber [{agent_endpoint_for(subscriber).lower()}]', N'-Subscriber [' + @SubscriberName + N']');",
        )
    ]
    lines = [
        "USE [msdb];",
        "GO",
        "-- Normalize generated agent job commands for SQL Authentication and reachable endpoints.",
    ]
    lines.extend(
        [
            "-- Stop continuously running agents before patching. Ignore already-stopped jobs.",
            "DECLARE @StopJobName sysname;",
            "DECLARE stop_cursor CURSOR LOCAL FAST_FORWARD FOR",
            "SELECT j.name",
            "FROM msdb.dbo.sysjobs AS j",
            "JOIN msdb.dbo.sysjobsteps AS s ON s.job_id = j.job_id",
            "JOIN msdb.dbo.sysjobactivity AS ja ON ja.job_id = j.job_id",
            "WHERE ja.session_id = (SELECT MAX(session_id) FROM msdb.dbo.syssessions)",
            "  AND ja.start_execution_date IS NOT NULL",
            "  AND ja.stop_execution_date IS NULL",
            f"  AND j.name LIKE {tsql_string(job_prefix)}",
            "  AND s.step_id = 2",
            "  AND s.subsystem IN (N'LogReader', N'Distribution')",
            "ORDER BY CASE WHEN s.subsystem = N'Distribution' THEN 0 ELSE 1 END, j.name;",
            "OPEN stop_cursor;",
            "FETCH NEXT FROM stop_cursor INTO @StopJobName;",
            "WHILE @@FETCH_STATUS = 0",
            "BEGIN",
            "    BEGIN TRY",
            "        EXEC msdb.dbo.sp_stop_job @job_name = @StopJobName;",
            "    END TRY",
            "    BEGIN CATCH",
            "        PRINT CONCAT('stop skipped for ', @StopJobName, ': ', ERROR_MESSAGE());",
            "    END CATCH;",
            "    FETCH NEXT FROM stop_cursor INTO @StopJobName;",
            "END",
            "CLOSE stop_cursor;",
            "DEALLOCATE stop_cursor;",
            "GO",
            "DECLARE @JobName sysname;",
            "DECLARE @StepId int;",
            "DECLARE @Subsystem nvarchar(40);",
            "DECLARE @Command nvarchar(max);",
            "DECLARE @NewCommand nvarchar(max);",
            "DECLARE @IsLogReader bit;",
            "DECLARE @PublisherName nvarchar(256);",
            "DECLARE @SubscriberName nvarchar(256);",
            f"DECLARE @PasswordValue nvarchar(4000) = {tsql_string(password_value)};",
            "DECLARE @OldTokenPassword nvarchar(4000) = N'[' + N'$' + N'(ReplicationPassword)]';",
            "IF DB_ID(N'distribution') IS NULL",
            "    THROW 53000, 'distribution database is missing; cannot discover registered Publisher name.', 1;",
            "SELECT TOP (1) @PublisherName = rs.srvname",
            "FROM distribution.dbo.MSpublisher_databases AS pdb",
            "JOIN distribution.dbo.MSreplservers AS rs ON rs.srvid = pdb.publisher_id",
            f"WHERE pdb.publisher_db = N'{config.database}'",
            f"ORDER BY CASE WHEN rs.srvname = N'{publisher.expected_sql_name}' THEN 0 WHEN rs.srvname = N'{publisher.replication_name}' THEN 1 ELSE 2 END;",
            f"IF @PublisherName IS NULL SET @PublisherName = N'{publisher.expected_sql_name}';",
            "DECLARE security_cursor CURSOR LOCAL FAST_FORWARD FOR",
            "SELECT j.name, s.step_id, s.subsystem, CONVERT(nvarchar(max), s.command)",
            "FROM msdb.dbo.sysjobs AS j",
            "JOIN msdb.dbo.sysjobsteps AS s ON s.job_id = j.job_id",
            f"WHERE j.name LIKE {tsql_string(job_prefix)}",
            "  AND s.step_id = 2",
            "  AND s.subsystem IN (N'LogReader', N'Distribution')",
            "ORDER BY CASE WHEN s.subsystem = N'LogReader' THEN 0 ELSE 1 END, j.name;",
            "OPEN security_cursor;",
            "FETCH NEXT FROM security_cursor INTO @JobName, @StepId, @Subsystem, @Command;",
            "WHILE @@FETCH_STATUS = 0",
            "BEGIN",
            "    SET @IsLogReader = CASE WHEN @Subsystem = N'LogReader' THEN 1 ELSE 0 END;",
            "    SET @SubscriberName = NULL;",
            "    SET @NewCommand = @Command;",
            "    -- -Publisher is a logical replication metadata name; SQL Server rejects raw tcp: endpoints here.",
            f"    SET @NewCommand = REPLACE(@NewCommand, N'-Publisher [{publisher_endpoint}]', N'-Publisher [' + @PublisherName + N']');",
            f"    SET @NewCommand = REPLACE(@NewCommand, N'-Publisher [{publisher.expected_sql_name}]', N'-Publisher [' + @PublisherName + N']');",
            f"    SET @NewCommand = REPLACE(@NewCommand, N'-Publisher [{publisher.expected_sql_name.lower()}]', N'-Publisher [' + @PublisherName + N']');",
            f"    SET @NewCommand = REPLACE(@NewCommand, N'-Publisher [{publisher.replication_name}]', N'-Publisher [' + @PublisherName + N']');",
            f"    SET @NewCommand = REPLACE(@NewCommand, N'-Publisher [{publisher.replication_name.lower()}]', N'-Publisher [' + @PublisherName + N']');",
            f"    SET @NewCommand = REPLACE(@NewCommand, N'-Distributor [{publisher.expected_sql_name}]', N'-Distributor [{publisher_endpoint}]');",
            f"    SET @NewCommand = REPLACE(@NewCommand, N'-Distributor [{publisher.expected_sql_name.lower()}]', N'-Distributor [{publisher_endpoint}]');",
            f"    SET @NewCommand = REPLACE(@NewCommand, N'-Distributor [{publisher.replication_name}]', N'-Distributor [{publisher_endpoint}]');",
            f"    SET @NewCommand = REPLACE(@NewCommand, N'-Distributor [{publisher.replication_name.lower()}]', N'-Distributor [{publisher_endpoint}]');",
            *subscriber_endpoint_checks,
            "    SET @NewCommand = REPLACE(@NewCommand, N'-DistributorSecurityMode 1', N'-DistributorSecurityMode 0');",
            "    SET @NewCommand = REPLACE(@NewCommand, N'-DistributorSecurityMode [1]', N'-DistributorSecurityMode 0');",
            "    SET @NewCommand = REPLACE(@NewCommand, N'-DistributorPassword ' + @OldTokenPassword, N'-DistributorPassword ' + @PasswordValue);",
            "    SET @NewCommand = REPLACE(@NewCommand, N'-PublisherPassword ' + @OldTokenPassword, N'-PublisherPassword ' + @PasswordValue);",
            "    SET @NewCommand = REPLACE(@NewCommand, N'-SubscriberPassword ' + @OldTokenPassword, N'-SubscriberPassword ' + @PasswordValue);",
            "    IF CHARINDEX(N'-DistributorSecurityMode', @NewCommand) = 0",
            "        SET @NewCommand = @NewCommand + N' -DistributorSecurityMode 0';",
            "    IF CHARINDEX(N'-DistributorLogin', @NewCommand) = 0",
            f"        SET @NewCommand = @NewCommand + N' -DistributorLogin [{tsql_bracket_value('sa')}]';",
            "    IF CHARINDEX(N'-DistributorPassword', @NewCommand) = 0",
            "        SET @NewCommand = @NewCommand + N' -DistributorPassword ' + @PasswordValue;",
            "    IF @IsLogReader = 1",
            "    BEGIN",
            "        SET @NewCommand = REPLACE(@NewCommand, N'-PublisherSecurityMode 1', N'-PublisherSecurityMode 0');",
            "        SET @NewCommand = REPLACE(@NewCommand, N'-PublisherSecurityMode [1]', N'-PublisherSecurityMode 0');",
            "        IF CHARINDEX(N'-PublisherSecurityMode', @NewCommand) = 0",
            "            SET @NewCommand = @NewCommand + N' -PublisherSecurityMode 0';",
            "        IF CHARINDEX(N'-PublisherLogin', @NewCommand) = 0",
            f"            SET @NewCommand = @NewCommand + N' -PublisherLogin [{tsql_bracket_value('sa')}]';",
            "        IF CHARINDEX(N'-PublisherPassword', @NewCommand) = 0",
            "            SET @NewCommand = @NewCommand + N' -PublisherPassword ' + @PasswordValue;",
            "    END",
            "    ELSE",
            "    BEGIN",
            "        SET @NewCommand = REPLACE(@NewCommand, N'-SubscriberSecurityMode 1', N'-SubscriberSecurityMode 0');",
            "        SET @NewCommand = REPLACE(@NewCommand, N'-SubscriberSecurityMode [1]', N'-SubscriberSecurityMode 0');",
            "        IF @SubscriberName IS NOT NULL",
            "        BEGIN",
            *subscriber_replacements,
            "        END",
            "        IF CHARINDEX(N'-SubscriberSecurityMode', @NewCommand) = 0",
            "            SET @NewCommand = @NewCommand + N' -SubscriberSecurityMode 0';",
            "        IF CHARINDEX(N'-SubscriberLogin', @NewCommand) = 0",
            f"            SET @NewCommand = @NewCommand + N' -SubscriberLogin [{tsql_bracket_value('sa')}]';",
            "        IF CHARINDEX(N'-SubscriberPassword', @NewCommand) = 0",
            "            SET @NewCommand = @NewCommand + N' -SubscriberPassword ' + @PasswordValue;",
            "    END",
            "    IF @NewCommand <> @Command",
            "    BEGIN",
            "        EXEC msdb.dbo.sp_update_jobstep @job_name = @JobName, @step_id = @StepId, @command = @NewCommand;",
            "        PRINT CONCAT('patched security for ', @JobName, ' step ', @StepId);",
            "    END",
            "    FETCH NEXT FROM security_cursor INTO @JobName, @StepId, @Subsystem, @Command;",
            "END",
            "CLOSE security_cursor;",
            "DEALLOCATE security_cursor;",
            "GO",
            "DECLARE @StartJobName sysname;",
            "DECLARE start_cursor CURSOR LOCAL FAST_FORWARD FOR",
            "SELECT j.name",
            "FROM msdb.dbo.sysjobs AS j",
            "JOIN msdb.dbo.sysjobsteps AS s ON s.job_id = j.job_id",
            "LEFT JOIN msdb.dbo.sysjobactivity AS ja ON ja.job_id = j.job_id",
            "  AND ja.session_id = (SELECT MAX(session_id) FROM msdb.dbo.syssessions)",
            f"WHERE j.name LIKE {tsql_string(job_prefix)}",
            "  AND s.step_id = 2",
            "  AND s.subsystem IN (N'LogReader', N'Distribution')",
            "  AND (ja.start_execution_date IS NULL OR ja.stop_execution_date IS NOT NULL)",
            "ORDER BY CASE WHEN s.subsystem = N'LogReader' THEN 0 ELSE 1 END, j.name;",
            "OPEN start_cursor;",
            "FETCH NEXT FROM start_cursor INTO @StartJobName;",
            "WHILE @@FETCH_STATUS = 0",
            "BEGIN",
            "    BEGIN TRY",
            "        EXEC msdb.dbo.sp_start_job @job_name = @StartJobName;",
            "        PRINT CONCAT('started ', @StartJobName);",
            "    END TRY",
            "    BEGIN CATCH",
            "        PRINT CONCAT('start skipped for ', @StartJobName, ': ', ERROR_MESSAGE());",
            "    END CATCH;",
            "    FETCH NEXT FROM start_cursor INTO @StartJobName;",
            "END",
            "CLOSE start_cursor;",
            "DEALLOCATE start_cursor;",
            "GO",
        ]
    )
    return "\n".join(lines)


def generate_client_alias_fix(config: ReplConfig, *, shell: str = "powershell") -> str:
    publisher = config.nodes[config.publisher]
    subscribers = [node for node in config.nodes.values() if node.role == "subscriber"]
    alias_rows = [
        (subscriber.expected_sql_name, subscriber.server.rsplit(",", 1)[0], endpoint_port(subscriber.server) or "1433")
        for subscriber in subscribers
    ]
    powershell_lines = [
        f"# Publisher: {publisher.expected_sql_name}",
        "$paths = @(",
        "  'HKLM:\\SOFTWARE\\Microsoft\\MSSQLServer\\Client\\ConnectTo',",
        "  'HKLM:\\SOFTWARE\\WOW6432Node\\Microsoft\\MSSQLServer\\Client\\ConnectTo'",
        ")",
        "$aliases = @(",
    ]
    for alias, host, port in alias_rows:
        powershell_lines.append(f"  @{{ Name = '{alias}'; Value = 'DBMSSOCN,{host},{port}' }}")
    powershell_lines.extend(
        [
            ")",
            "foreach ($path in $paths) {",
            "  New-Item -Path $path -Force | Out-Null",
            "  foreach ($alias in $aliases) {",
            "    New-ItemProperty -Path $path -Name $alias.Name -PropertyType String -Value $alias.Value -Force | Out-Null",
            "    Write-Host \"Set $($alias.Name) -> $($alias.Value) in $path\"",
            "  }",
            "}",
        ]
    )
    if shell == "cmd":
        lines = [
            "@echo off",
            "REM Run as Administrator on the NODE010 SQL Agent host.",
            f"REM Publisher: {publisher.expected_sql_name}",
        ]
        for alias, host, port in alias_rows:
            value = f"DBMSSOCN,{host},{port}"
            lines.extend(
                [
                    f'reg add "HKLM\\SOFTWARE\\Microsoft\\MSSQLServer\\Client\\ConnectTo" /v "{alias}" /t REG_SZ /d "{value}" /f',
                    f'reg add "HKLM\\SOFTWARE\\WOW6432Node\\Microsoft\\MSSQLServer\\Client\\ConnectTo" /v "{alias}" /t REG_SZ /d "{value}" /f',
                ]
            )
        return "\n".join(lines)

    if shell == "wsl":
        ps_script = "\n".join(powershell_lines)
        indented_script = "\n".join(f"  {line}" for line in ps_script.splitlines())
        return "\n".join(
            [
                "#!/usr/bin/env bash",
                "set -euo pipefail",
                "# Run from WSL on NODE010. The WSL session must be elevated as Administrator.",
                "is_admin=$(powershell.exe -NoProfile -Command \"([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)\" | tr -d '\\r')",
                "if [[ \"$is_admin\" != \"True\" ]]; then",
                "  echo \"This writes HKLM SQL client aliases. Start WSL as Administrator on NODE010 and rerun.\" >&2",
                "  exit 1",
                "fi",
                "tmp_ps=$(mktemp --suffix=.ps1)",
                "trap 'rm -f \"$tmp_ps\"' EXIT",
                "cat > \"$tmp_ps\" <<'POWERSHELL'",
                indented_script,
                "POWERSHELL",
                "win_ps=$(wslpath -w \"$tmp_ps\")",
                "powershell.exe -NoProfile -ExecutionPolicy Bypass -File \"$win_ps\"",
            ]
        )

    lines = [
        "# Run as Administrator on the NODE010 SQL Agent host.",
        *powershell_lines,
    ]
    return "\n".join(lines)


def replication_agent_job_names(config: ReplConfig) -> list[str]:
    publisher = config.nodes[config.publisher]
    subscribers = [node for node in config.nodes.values() if node.role == "subscriber"]
    names = [f"{publisher.expected_sql_name}-{config.database}-1"]
    names.extend(replication_agent_job_name(config, subscriber, index) for index, subscriber in enumerate(subscribers, start=1))
    return names


def replication_agent_job_name(config: ReplConfig, subscriber: Node, index: int) -> str:
    publisher = config.nodes[config.publisher]
    return f"{publisher.expected_sql_name}-{config.database}-{config.publication}-{subscriber.expected_sql_name}-{index}"


def generate_name_fix_sql(config: ReplConfig, node_names: list[str] | None) -> str:
    selected = node_names or sorted(config.nodes)
    lines = [
        "-- Run each block on the matching SQL Server node, then restart that SQL Server instance.",
        "-- Replication should not be configured until every node reports a unique @@SERVERNAME.",
    ]
    for node_name in selected:
        node = config.nodes.get(node_name)
        if node is None:
            raise ReplError(f"unknown node: {node_name}")
        lines.extend(
            [
                "",
                f"-- {node.name} ({node.server}) should become {node.expected_sql_name}",
                "USE [master];",
                "GO",
                f"IF @@SERVERNAME <> N'{node.expected_sql_name}'",
                "BEGIN",
                "    DECLARE @OldServerName sysname = @@SERVERNAME;",
                "    EXEC sys.sp_dropserver @server = @OldServerName;",
                f"    EXEC sys.sp_addserver @server = N'{node.expected_sql_name}', @local = N'local';",
                f"    PRINT 'Restart SQL Server on {node.name}, then verify @@SERVERNAME = {node.expected_sql_name}';",
                "END",
                "ELSE",
                "BEGIN",
                f"    PRINT '{node.name} already has the expected logical SQL Server name.';",
                "END",
                "GO",
            ]
        )
    return "\n".join(lines)


def one_row(connection: Any, sql: str, *params: object) -> dict[str, Any]:
    rows = all_rows(connection, sql, *params)
    if not rows:
        raise ReplError("query returned no rows")
    return rows[0]


def all_rows(connection: Any, sql: str, *params: object) -> list[dict[str, Any]]:
    cursor = connection.cursor()
    try:
        cursor.execute(sql, *params)
        columns = [column[0] for column in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
    finally:
        cursor.close()


def q(identifier: str) -> str:
    return "[" + identifier.replace("]", "]]") + "]"


def tsql_string(value: str) -> str:
    return "N'" + value.replace("'", "''") + "'"


def tsql_bracket_value(value: str) -> str:
    return value.replace("]", "]]")


def format_preflight(report: dict[str, Any]) -> str:
    lines = [f"WGD replication preflight for {report['database']} / {report['publication']}"]
    for node in report["nodes"]:
        lines.append("")
        lines.append(f"{node['node']} {node['server']} ({node['role']})")
        if not node.get("connect_ok"):
            lines.append(f"  connect: FAILED {node.get('error')}")
            continue
        lines.append(f"  connect: ok")
        lines.append(
            f"  sql_name: {node.get('sql_name')} expected: {node.get('expected_sql_name')} "
            f"replication: {node.get('replication_name')}"
        )
        lines.append(f"  machine: {node.get('machine_name')}")
        lines.append(f"  edition: {node.get('edition')} version: {node.get('product_version')}")
        lines.append(f"  backup_path: {node.get('backup_path')}")
        database = node.get("database", {})
        lines.append(f"  database: {database.get('state_desc')} {database.get('recovery_model_desc')} {database.get('size_mb')} MB")
        agent = next((svc for svc in node.get("services", []) if str(svc.get("servicename", "")).startswith("SQL Server Agent")), None)
        if agent:
            lines.append(f"  agent: {agent.get('status_desc')} ({agent.get('startup_type_desc')})")
        counts = node.get("table_counts", {})
        lines.append(f"  wgd tables: {len(counts)}/{len(WGD_TABLES)}")
    lines.append("")
    lines.append("Blockers:")
    if report["blockers"]:
        lines.extend(f"  - {issue}" for issue in report["blockers"])
    else:
        lines.append("  <none>")
    return "\n".join(lines)


def format_backup_results(results: list[dict[str, Any]]) -> str:
    lines = ["WGD backup results"]
    for result in results:
        if result.get("ok"):
            lines.append(f"  {result['node']}: {result['backup_path']}")
        else:
            lines.append(f"  {result['node']}: FAILED {result.get('error')}")
    return "\n".join(lines)


if __name__ == "__main__":
    raise SystemExit(main())
