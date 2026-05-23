#!/usr/bin/env python3
"""Dark Tkinter monitor for the WGD SQL Server replication cluster."""

from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
import re
import sys
import threading
import time
import tkinter as tk
from tkinter import ttk
from typing import Any, Callable, Protocol


CURRENT_DIR = Path(__file__).resolve().parent
if str(CURRENT_DIR) not in sys.path:
    sys.path.insert(0, str(CURRENT_DIR))

from wgd_replication import (  # noqa: E402
    DEFAULT_CONFIG,
    WGD_TABLES,
    ReplConfig,
    all_rows,
    connect,
    database_properties,
    load_repl_config,
    server_properties,
    services,
    table_counts,
)


APP_NAME = "Dark Cluster Watch"
APP_VERSION = "0.1.0"
APP_TITLE = f"{APP_NAME} v{APP_VERSION}"
LOG_READER_JOB = "NODE010-POVIID-1"
NODE020_DISTRIBUTION_JOB = "NODE010-POVIID-WGD_TMP_Publication-NODE020-1"
NODE030_DISTRIBUTION_JOB = "NODE010-POVIID-WGD_TMP_Publication-NODE030-2"
EXPECTED_NODE_NAMES = ("NODE010", "NODE020", "NODE030")
UNQUALIFIED_DISTRIBUTOR = "NODE010"
START_JOB_ORDER = (LOG_READER_JOB, NODE020_DISTRIBUTION_JOB, NODE030_DISTRIBUTION_JOB)
STOP_JOB_ORDER = (NODE020_DISTRIBUTION_JOB, NODE030_DISTRIBUTION_JOB, LOG_READER_JOB)
DISTRIBUTION_JOB_SUBSCRIBERS = {
    NODE020_DISTRIBUTION_JOB: "NODE020",
    NODE030_DISTRIBUTION_JOB: "NODE030",
}
MIN_REFRESH_SECONDS = 5
MAX_REFRESH_SECONDS = 900
HEALTH_OK = "O.K."
BAD_JOB_RUN_STATUSES = {0, 2, 3}
UNHEALTHY_JOB_MESSAGE_TOKENS = ("error", "fail", "retry", "refused", "cannot", "unable")
ROOT_CAUSE_TOKENS = (
    "could not connect",
    "could not open",
    "named pipes",
    "query timeout",
    "server is not found",
    "failed command",
    "job step contains tokens",
    "agent message code 20084",
    "cannot generate sspi context",
    "is not registered at server",
    "login failed",
    "network-related",
    "not been designated as a valid publisher",
    "timeout expired",
)
AGENT_SQL_AUTH_FIX_MESSAGE = (
    "Replication agents are using Windows Authentication. Recreate replication with the SQL password "
    "so generated agents use SQL Authentication."
)
AGENT_TOKEN_PLACEHOLDER_MESSAGE = (
    "Replication agent job steps contain SQL Agent token placeholders. Recreate replication with the SQL password "
    "so generated agent commands contain plain bracketed password values."
)
LOG_TOKEN_RE = re.compile(
    r"(?<![\w.])"
    r"(O\.K\.|ok|warning|retry|stopped|error|failed|refused|cannot|unable|health|check|restart|refresh|snapshot|started|done|start|stop)"
    r"(?![\w.])",
    re.IGNORECASE,
)

COLORS = {
    "bg": "#101418",
    "panel": "#171d23",
    "panel_alt": "#1e2630",
    "border": "#2c3946",
    "text": "#e6edf3",
    "muted": "#9aa7b2",
    "accent": "#56b6c2",
    "good": "#65d487",
    "warn": "#e5c07b",
    "bad": "#ff6b6b",
    "button": "#26313d",
    "button_active": "#314154",
}

LOG_COLORS = {
    "log_timestamp": "#7f91a7",
    "log_text": "#aeb7c0",
    "log_action": "#64bfd7",
    "log_ok": "#6edb8f",
    "log_warn": "#e3bf6a",
    "log_error": "#ff7d7d",
}


@dataclass(frozen=True)
class NodeStatus:
    name: str
    server: str
    connected: bool
    sql_name: str = ""
    machine_name: str = ""
    edition: str = ""
    database_state: str = ""
    recovery_model: str = ""
    wgd_table_count: int = 0
    wgd_total_rows: int = 0
    error: str = ""


@dataclass(frozen=True)
class JobStatus:
    name: str
    category: str = ""
    subsystem: str = ""
    agent_id: int | None = None
    enabled: bool = False
    running: bool = False
    replication_running: bool | None = None
    replication_message: str = ""
    start_execution_date: str = ""
    stop_execution_date: str = ""
    last_run_status: int | None = None
    last_run_message: str = ""


@dataclass(frozen=True)
class ReplicationJobInfo:
    name: str
    subsystem: str = ""
    agent_id: int | None = None


@dataclass(frozen=True)
class JobActionResult:
    job_name: str
    command: str
    ok: bool
    error: str = ""


@dataclass(frozen=True)
class ClusterSnapshot:
    nodes: list[NodeStatus]
    jobs: list[JobStatus]
    generated_at: datetime = field(default_factory=datetime.now)
    errors: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class HealthIndicator:
    label: str
    color: str
    detail: str = ""


@dataclass(frozen=True)
class HealthFinding:
    severity: str
    message: str
    source: str = ""


class ClusterAdapter(Protocol):
    def collect_node_statuses(self, config: ReplConfig) -> list[NodeStatus]:
        ...

    def collect_job_statuses(self, config: ReplConfig) -> list[JobStatus]:
        ...

    def collect_health_findings(self, config: ReplConfig) -> list[HealthFinding]:
        ...

    def run_job_command(self, config: ReplConfig, command: str, job_names: tuple[str, ...]) -> list[JobActionResult]:
        ...


class SqlClusterAdapter:
    """SQL Server-backed cluster adapter."""

    def collect_node_statuses(self, config: ReplConfig) -> list[NodeStatus]:
        statuses: list[NodeStatus] = []
        for node in config.nodes.values():
            try:
                connection = connect(node, config, timeout=8)
            except Exception as exc:
                statuses.append(NodeStatus(name=node.name, server=node.server, connected=False, error=str(exc)))
                continue
            try:
                props = server_properties(connection)
                db = database_properties(connection, config.database)["database"]
                counts = table_counts(connection, config)
                statuses.append(
                    NodeStatus(
                        name=node.name,
                        server=node.server,
                        connected=True,
                        sql_name=str(props.get("sql_name") or ""),
                        machine_name=str(props.get("machine_name") or ""),
                        edition=str(props.get("edition") or ""),
                        database_state=str(db.get("state_desc") or ""),
                        recovery_model=str(db.get("recovery_model_desc") or ""),
                        wgd_table_count=len(counts),
                        wgd_total_rows=sum(counts.values()),
                    )
                )
            except Exception as exc:
                statuses.append(NodeStatus(name=node.name, server=node.server, connected=False, error=str(exc)))
            finally:
                connection.close()
        return statuses

    def collect_job_statuses(self, config: ReplConfig) -> list[JobStatus]:
        publisher = config.nodes[config.publisher]
        connection = connect(publisher, config, database="msdb", timeout=8)
        try:
            jobs = read_current_replication_jobs(connection, config)
            return read_job_statuses(connection, jobs)
        finally:
            connection.close()

    def collect_health_findings(self, config: ReplConfig) -> list[HealthFinding]:
        errors: list[str] = []
        nodes: list[NodeStatus] = []
        jobs: list[JobStatus] = []
        job_history: dict[str, list[dict[str, Any]]] = {}
        publisher_info: dict[str, Any] = {}
        registered_servers: list[dict[str, Any]] = []
        job_commands: list[dict[str, Any]] = []
        job_names: tuple[str, ...] = START_JOB_ORDER
        repl_error_floor: datetime | None = None
        logreader_history: list[dict[str, Any]] = []
        distribution_history: list[dict[str, Any]] = []
        repl_errors: list[dict[str, Any]] = []

        try:
            nodes = self.collect_node_statuses(config)
        except Exception as exc:
            errors.append(f"Nodes: {exc}")
        try:
            jobs = self.collect_job_statuses(config)
            job_names = tuple(job.name for job in jobs) or START_JOB_ORDER
        except Exception as exc:
            errors.append(f"Replication jobs: {exc}")

        publisher = config.nodes[config.publisher]
        try:
            connection = connect(publisher, config, database="master", timeout=8)
            try:
                publisher_info = read_publisher_host_info(connection)
                registered_servers = read_registered_servers(connection)
            finally:
                connection.close()
        except Exception as exc:
            errors.append(f"Publisher metadata: {exc}")

        try:
            connection = connect(publisher, config, database="msdb", timeout=8)
            try:
                job_names = read_current_replication_job_names(connection, config)
                job_history = read_job_history_details(connection, job_names)
                job_commands = read_replication_job_commands(connection, job_names)
            finally:
                connection.close()
        except Exception as exc:
            errors.append(f"Job history: {exc}")

        try:
            connection = connect(publisher, config, database="distribution", timeout=8)
            try:
                logreader_agent_ids = current_agent_ids(jobs, "LogReader")
                distribution_agent_ids = current_agent_ids(jobs, "Distribution")
                repl_error_floor = read_current_agent_generation_floor(
                    connection,
                    logreader_agent_ids=logreader_agent_ids,
                    distribution_agent_ids=distribution_agent_ids,
                )
                logreader_history = read_logreader_history(connection, agent_ids=logreader_agent_ids)
                distribution_history = read_distribution_history(connection, agent_ids=distribution_agent_ids)
                repl_errors = read_repl_errors(connection, after=repl_error_floor)
            finally:
                connection.close()
        except Exception as exc:
            errors.append(f"Distribution history: {exc}")

        return build_cluster_health_findings(
            nodes,
            jobs,
            port_qualified_distributor=port_qualified_distributor(config),
            unqualified_distributor=publisher_sql_name(config),
            errors=errors,
            job_history=job_history,
            expected_job_names=job_names,
            repl_error_floor=repl_error_floor,
            publisher_info=publisher_info,
            registered_servers=registered_servers,
            job_commands=job_commands,
            logreader_history=logreader_history,
            distribution_history=distribution_history,
            repl_errors=repl_errors,
        )

    def run_job_command(self, config: ReplConfig, command: str, job_names: tuple[str, ...]) -> list[JobActionResult]:
        if command not in {"start", "stop"}:
            raise ValueError(f"unsupported job command: {command}")
        publisher = config.nodes[config.publisher]
        connection = connect(publisher, config, database="msdb", timeout=8)
        try:
            actual_job_names = read_current_replication_job_names(connection, config)
            if command == "stop":
                actual_job_names = tuple(reversed(actual_job_names))
            return execute_job_command(connection, command, actual_job_names or job_names)
        finally:
            connection.close()


def port_qualified_distributor(config: ReplConfig) -> str:
    return config.nodes[config.publisher].replication_name


def publisher_sql_name(config: ReplConfig) -> str:
    return config.nodes[config.publisher].expected_sql_name


class ClusterService:
    def __init__(self, config_path: Path = DEFAULT_CONFIG, adapter: ClusterAdapter | None = None) -> None:
        self.config_path = Path(config_path)
        self.config = load_repl_config(self.config_path)
        self.adapter = adapter or SqlClusterAdapter()

    def refresh(self) -> ClusterSnapshot:
        errors: list[str] = []
        nodes: list[NodeStatus] = []
        jobs: list[JobStatus] = []
        try:
            nodes = self.adapter.collect_node_statuses(self.config)
        except Exception as exc:
            errors.append(f"Nodes: {exc}")
        try:
            jobs = self.adapter.collect_job_statuses(self.config)
        except Exception as exc:
            errors.append(f"Replication jobs: {exc}")
        return ClusterSnapshot(nodes=nodes, jobs=jobs, errors=errors)

    def start_replication(self) -> list[JobActionResult]:
        return self.adapter.run_job_command(self.config, "start", START_JOB_ORDER)

    def stop_replication(self) -> list[JobActionResult]:
        return self.adapter.run_job_command(self.config, "stop", STOP_JOB_ORDER)

    def health_check(self) -> list[HealthFinding]:
        return self.adapter.collect_health_findings(self.config)

    def restart_replication(
        self,
        *,
        sleeper: Callable[[float], None] = time.sleep,
        delay_seconds: float = 5.0,
    ) -> list[JobActionResult]:
        results = self.stop_replication()
        sleeper(delay_seconds)
        results.extend(self.start_replication())
        return results


def clamp_interval(value: object) -> int:
    try:
        parsed = int(float(str(value)))
    except (TypeError, ValueError):
        return MIN_REFRESH_SECONDS
    return max(MIN_REFRESH_SECONDS, min(MAX_REFRESH_SECONDS, parsed))


def job_state_label(job: JobStatus) -> tuple[str, str]:
    if not job.enabled:
        return "DISABLED", COLORS["muted"]
    if is_distribution_job(job) and job.replication_running is not None:
        if job.replication_running:
            return "RUNNING", COLORS["good"]
        return "FAILED", COLORS["bad"]
    if job.running:
        return "RUNNING", COLORS["good"]
    if job.last_run_status == 0:
        return "FAILED", COLORS["bad"]
    if job.last_run_status == 2:
        return "RETRY", COLORS["warn"]
    return "STOPPED", COLORS["muted"]


def connectivity_indicator(nodes: list[NodeStatus], errors: list[str] | None = None) -> HealthIndicator:
    node_errors = [error for error in errors or [] if str(error).lower().startswith("nodes:")]
    if node_errors:
        return HealthIndicator("Error", COLORS["bad"], node_errors[0])

    by_name = {node.name: node for node in nodes}
    missing = [name for name in EXPECTED_NODE_NAMES if name not in by_name]
    if missing:
        return HealthIndicator("Error", COLORS["bad"], f"missing nodes: {', '.join(missing)}")

    problems = [problem for name in EXPECTED_NODE_NAMES if (problem := node_connectivity_problem(by_name[name]))]
    if problems:
        has_offline = any("offline" in problem.lower() for problem in problems)
        return HealthIndicator("Error" if has_offline else "Warning", COLORS["bad"] if has_offline else COLORS["warn"], "; ".join(problems))
    return HealthIndicator(HEALTH_OK, COLORS["good"])


def node_connectivity_problem(status: NodeStatus) -> str:
    if not status.connected:
        return f"{status.name} offline"
    if status.database_state.upper() != "ONLINE":
        return f"{status.name} POVIID {status.database_state or 'unknown'}"
    if status.wgd_table_count < len(WGD_TABLES):
        return f"{status.name} WGD tables {status.wgd_table_count}/{len(WGD_TABLES)}"
    return ""


def node_status_indicator(status: NodeStatus) -> HealthIndicator:
    if not status.connected:
        return HealthIndicator("OFFLINE", COLORS["bad"], status.error or f"{status.name} offline")
    problem = node_connectivity_problem(status)
    if problem:
        return HealthIndicator("Warning", COLORS["warn"], problem)
    return HealthIndicator(HEALTH_OK, COLORS["good"])


def sync_indicator(jobs: list[JobStatus], errors: list[str] | None = None) -> HealthIndicator:
    job_errors = [error for error in errors or [] if str(error).lower().startswith("replication jobs:")]
    if job_errors:
        return HealthIndicator("Error", COLORS["bad"], job_errors[0])

    by_name = {job.name: job for job in jobs}
    problems: list[str] = []
    expected_job_names = tuple(job.name for job in jobs) or START_JOB_ORDER
    for job_name in expected_job_names:
        job = by_name.get(job_name)
        if job is None:
            problems.append(f"{job_name} missing")
            continue
        if problem := job_health_problem(job):
            problems.append(f"{job_name} {problem}")
    if problems:
        return HealthIndicator("Warning", COLORS["warn"], "; ".join(problems))
    return HealthIndicator(HEALTH_OK, COLORS["good"])


def is_job_healthy(job: JobStatus) -> bool:
    return job_health_problem(job) == ""


def job_health_problem(job: JobStatus) -> str:
    if not job.enabled:
        return "disabled"
    if is_distribution_job(job) and job.replication_running is not None:
        if not job.replication_running:
            return "replication not running"
        if has_unhealthy_job_message(job.replication_message):
            return "latest message warning"
        return ""
    if not job.running:
        return "not running"
    if job.last_run_status in BAD_JOB_RUN_STATUSES:
        return f"last status {job.last_run_status}"
    if has_unhealthy_job_message(job.last_run_message):
        return "latest message warning"
    return ""


def has_unhealthy_job_message(message: str) -> bool:
    lower = str(message or "").lower()
    return any(token in lower for token in UNHEALTHY_JOB_MESSAGE_TOKENS)


def is_distribution_job(job: JobStatus) -> bool:
    return job.subsystem.lower() == "distribution"


def job_status_message(job: JobStatus) -> str:
    if is_distribution_job(job) and job.replication_message:
        return job.replication_message
    return job.last_run_message


def job_display_message(job: JobStatus) -> str:
    if is_job_healthy(job):
        return compact_message(job.replication_message) if is_distribution_job(job) and job.replication_message else HEALTH_OK
    return compact_message(job_status_message(job)) or job_health_problem(job)


def important_job_log(jobs: list[JobStatus]) -> str:
    candidates = []
    for index, job in enumerate(jobs):
        message = compact_message(job_status_message(job))
        problem = job_health_problem(job)
        if message or problem or is_job_healthy(job):
            candidates.append((job_log_priority(job), index, job))
    if not candidates:
        return ""

    _priority, _index, job = max(candidates, key=lambda item: (item[0], item[1]))
    message = compact_message(job_status_message(job)) or (HEALTH_OK if is_job_healthy(job) else job_health_problem(job))
    return f"Last replication log {job.name}: {message}"


def job_log_priority(job: JobStatus) -> int:
    problem = job_health_problem(job)
    message = job_status_message(job)
    if problem and (job.last_run_status in BAD_JOB_RUN_STATUSES or has_unhealthy_job_message(message)):
        return 3
    if problem:
        return 2
    if message:
        return 1
    return 0


def build_cluster_health_findings(
    nodes: list[NodeStatus],
    jobs: list[JobStatus],
    *,
    port_qualified_distributor: str,
    unqualified_distributor: str = UNQUALIFIED_DISTRIBUTOR,
    errors: list[str] | None = None,
    job_history: dict[str, list[dict[str, Any]]] | None = None,
    expected_job_names: tuple[str, ...] = START_JOB_ORDER,
    repl_error_floor: datetime | None = None,
    publisher_info: dict[str, Any] | None = None,
    registered_servers: list[dict[str, Any]] | None = None,
    job_commands: list[dict[str, Any]] | None = None,
    logreader_history: list[dict[str, Any]] | None = None,
    distribution_history: list[dict[str, Any]] | None = None,
    repl_errors: list[dict[str, Any]] | None = None,
) -> list[HealthFinding]:
    findings: list[HealthFinding] = []
    seen: set[tuple[str, str, str]] = set()

    def add(severity: str, message: str, source: str = "") -> None:
        key = (severity, message, source)
        if key not in seen:
            findings.append(HealthFinding(severity, message, source))
            seen.add(key)

    for error in errors or []:
        add("Error", str(error), "collection")

    by_node = {node.name: node for node in nodes}
    for node_name in EXPECTED_NODE_NAMES:
        node = by_node.get(node_name)
        if node is None:
            add("Error", f"{node_name}: missing from health snapshot", node_name)
            continue
        problem = node_connectivity_problem(node)
        if problem:
            add("Error" if not node.connected else "Warning", problem, node_name)
        else:
            add(HEALTH_OK, f"{node_name}: node healthy", node_name)

    by_job = {job.name: job for job in jobs}
    root_cause_jobs = set()
    history_problem_jobs = set()
    token_problem = any(command_has_sql_agent_token(str(row.get("command") or "")) for row in job_commands or [])
    if token_problem:
        add("Error", AGENT_TOKEN_PLACEHOLDER_MESSAGE, "replication agent tokens")
    for job_name, rows in (job_history or {}).items():
        job = by_job.get(job_name)
        if job is not None and is_distribution_job(job) and job.replication_running is True:
            continue
        if latest_job_history_has_problem(rows):
            history_problem_jobs.add(job_name)
        for line in first_root_cause_lines(rows):
            root_cause_jobs.add(job_name)
            finding = root_cause_finding(
                job_name,
                line,
                port_qualified_distributor=port_qualified_distributor,
                unqualified_distributor=unqualified_distributor,
                prefer_token_placeholder=token_problem,
            )
            add(finding.severity, finding.message, finding.source)
            break

    for finding in linux_unqualified_replication_findings(
        publisher_info or {},
        registered_servers or [],
        job_commands or [],
        port_qualified_distributor=port_qualified_distributor,
        unqualified_distributor=unqualified_distributor,
    ):
        add(finding.severity, finding.message, finding.source)
    for finding in replication_agent_auth_findings(job_commands or [], token_problem=token_problem):
        add(finding.severity, finding.message, finding.source)
    for finding in replication_agent_endpoint_findings(
        job_commands or [],
        port_qualified_distributor=port_qualified_distributor,
        unqualified_distributor=unqualified_distributor,
    ):
        add(finding.severity, finding.message, finding.source)

    for job_name in expected_job_names:
        job = by_job.get(job_name)
        if job is None:
            add("Error", f"{job_name}: replication job missing", job_name)
            continue
        problem = job_health_problem(job)
        if problem and job_name not in root_cause_jobs:
            severity = "Error" if problem in {"disabled", "not running", "replication not running"} else "Warning"
            add(severity, f"{job_name}: {job_display_message(job)}", job_name)
        elif not problem and job_name not in history_problem_jobs:
            add(HEALTH_OK, f"{job_name}: replication job running", job_name)

    logreader_history_rows = logreader_history or []
    distribution_history_rows = distribution_history or []

    for row in latest_agent_history_warnings(logreader_history_rows, "name"):
        add("Warning", f"{row.get('name')}: {compact_message(row.get('comments', ''))}", str(row.get("name") or "logreader"))
    for row in latest_agent_history_warnings(distribution_history_rows, "name"):
        subscriber = str(row.get("subscriber_name") or "").upper()
        agent = str(row.get("name") or "distribution")
        job = by_job.get(agent)
        if job is not None and job.replication_running is True and benign_distribution_shutdown_history(row):
            continue
        prefix = f"{agent}" + (f" ({subscriber})" if subscriber else "")
        add("Warning", f"{prefix}: {compact_message(row.get('comments', ''))}", agent)

    for row in current_repl_errors(
        repl_errors or [],
        repl_error_floor,
        logreader_history=logreader_history_rows,
        distribution_history=distribution_history_rows,
    ):
        source = str(row.get("source_name") or "MSrepl_errors")
        error_text = compact_message(str(row.get("error_text") or "replication error"), 260)
        add("Error", f"{source}: {error_text}", source)

    if not any(finding.severity in {"Warning", "Error"} for finding in findings):
        findings.insert(0, HealthFinding(HEALTH_OK, "Cluster health check O.K.", "cluster"))
    return findings


def first_root_cause_lines(rows: list[dict[str, Any]]) -> list[str]:
    for row in rows:
        lines = extract_root_cause_lines(str(row.get("message") or ""))
        if lines:
            return lines
    return []


def latest_job_history_has_problem(rows: list[dict[str, Any]]) -> bool:
    for row in rows:
        status = row.get("run_status")
        message = str(row.get("message") or "")
        return status in BAD_JOB_RUN_STATUSES or has_unhealthy_job_message(message) or root_cause_message(message)
    return False


def extract_root_cause_lines(message: str) -> list[str]:
    lines = []
    for line in str(message or "").replace("\r", "").split("\n"):
        compact = " ".join(line.split())
        if not compact:
            continue
        lower = compact.lower()
        if root_cause_message(compact):
            lines.append(compact)
    return lines


def root_cause_message(message: str) -> bool:
    lower = str(message or "").lower()
    return any(token in lower for token in ROOT_CAUSE_TOKENS)


def root_cause_finding(
    job_name: str,
    line: str,
    *,
    port_qualified_distributor: str,
    unqualified_distributor: str = UNQUALIFIED_DISTRIBUTOR,
    prefer_token_placeholder: bool = False,
) -> HealthFinding:
    if agent_token_problem(line) or prefer_token_placeholder and agent_auth_problem(line):
        return HealthFinding("Error", AGENT_TOKEN_PLACEHOLDER_MESSAGE, "replication agent tokens")
    if agent_auth_problem(line):
        return HealthFinding("Error", AGENT_SQL_AUTH_FIX_MESSAGE, job_name)
    distributor = parse_quoted_target(line, "Distributor") or unqualified_distributor
    subscriber = distribution_job_subscriber(job_name)
    if "could not connect to distributor" in line.lower() and subscriber:
        if distributor == port_qualified_distributor or "," in distributor:
            return HealthFinding(
                "Error",
                f"Distribution agent {subscriber} cannot connect to port-qualified Distributor {distributor}. "
                "Check replication agent SQL Authentication; recreate replication with the SQL password.",
                job_name,
            )
        return HealthFinding(
            "Error",
            f"Distribution agent {subscriber} cannot connect to Distributor {distributor}. "
            f"Recreate replication so agents use explicit TCP endpoints for {port_qualified_distributor}; "
            f"/etc/hosts alone is not enough while metadata still uses {unqualified_distributor}.",
            job_name,
        )
    if "could not connect to subscriber" in line.lower() and subscriber:
        return HealthFinding(
            "Error",
            f"Distribution agent {subscriber} cannot connect to Subscriber. Configure SQL client aliases on the "
            "NODE010 SQL Agent host so subscriber logical names resolve to their non-default TCP ports.",
            job_name,
        )
    if "not been designated as a valid publisher" in line.lower():
        return HealthFinding(
            "Error",
            "Replication agent is using an invalid raw or unregistered Publisher name. "
            "Patch agent jobs so -Publisher uses the registered logical Publisher name from distribution metadata.",
            job_name,
        )
    if "is not registered at server" in line.lower():
        return HealthFinding(
            "Error",
            "Replication agent is using an invalid raw or unregistered Subscriber name. "
            "Patch agent jobs so -Subscriber uses the registered logical Subscriber name from distribution metadata.",
            job_name,
        )
    if is_log_reader_job_name(job_name):
        return HealthFinding(
            "Error",
            "Log Reader cannot connect while validating Publisher. The job should use "
            "the registered logical Publisher name, and the SQL Agent host must resolve "
            f"{unqualified_distributor} to the publisher address before the Log Reader is restarted.",
            job_name,
        )
    if subscriber:
        return HealthFinding("Error", f"Distribution agent {subscriber}: {compact_message(line, 260)}", job_name)
    return HealthFinding("Error", f"{job_name}: {compact_message(line, 260)}", job_name)


def distribution_job_subscriber(job_name: str) -> str:
    if job_name in DISTRIBUTION_JOB_SUBSCRIBERS:
        return DISTRIBUTION_JOB_SUBSCRIBERS[job_name]
    match = re.search(r"-([A-Z]+[0-9]+)-\d+$", job_name, re.IGNORECASE)
    return match.group(1).upper() if match else ""


def is_log_reader_job_name(job_name: str) -> bool:
    if job_name == LOG_READER_JOB:
        return True
    return bool(re.fullmatch(r"[A-Z]+[0-9]+-[^-]+-\d+", job_name, re.IGNORECASE))


def parse_quoted_target(message: str, label: str) -> str:
    match = re.search(rf"{re.escape(label)}\s+'([^']+)'", message, re.IGNORECASE)
    return match.group(1) if match else ""


def linux_unqualified_replication_findings(
    publisher_info: dict[str, Any],
    registered_servers: list[dict[str, Any]],
    job_commands: list[dict[str, Any]],
    *,
    port_qualified_distributor: str,
    unqualified_distributor: str = UNQUALIFIED_DISTRIBUTOR,
) -> list[HealthFinding]:
    if str(publisher_info.get("host_platform") or "").lower() != "linux":
        return []

    findings = []
    if any(command_uses_unqualified_distributor(str(row.get("command") or ""), unqualified_distributor) for row in job_commands):
        findings.append(
            HealthFinding(
                "Error",
                f"Linux replication jobs still use unqualified Distributor {unqualified_distributor}. "
                f"Recreate replication with the SQL password so agents use {port_qualified_distributor}.",
                "replication metadata",
            )
        )
    for row in registered_servers:
        name = str(row.get("name") or "")
        data_source = str(row.get("data_source") or "")
        provider_string = str(row.get("provider_string") or "")
        server_id = int(row.get("server_id") or 0)
        if (
            server_id != 0
            and name in {unqualified_distributor, "repl_distributor"}
            and data_source == unqualified_distributor
            and not provider_string
        ):
            findings.append(
                HealthFinding(
                    "Warning",
                    f"Registered server {name} points to unqualified {data_source}. "
                    f"Recreate replication metadata with explicit TCP endpoints for {port_qualified_distributor}.",
                    "sys.servers",
                )
            )
            break
    return findings


def command_uses_unqualified_distributor(command: str, unqualified_distributor: str = UNQUALIFIED_DISTRIBUTOR) -> bool:
    return bool(re.search(rf"-Distributor\s+\[?{re.escape(unqualified_distributor)}\]?(?!,)", command, re.IGNORECASE))


def replication_agent_auth_findings(job_commands: list[dict[str, Any]], *, token_problem: bool = False) -> list[HealthFinding]:
    if token_problem:
        return []
    if any(command_uses_windows_auth(str(row.get("command") or "")) for row in job_commands):
        return [HealthFinding("Error", AGENT_SQL_AUTH_FIX_MESSAGE, "replication agent security")]
    return []


def replication_agent_endpoint_findings(
    job_commands: list[dict[str, Any]],
    *,
    port_qualified_distributor: str = "",
    unqualified_distributor: str = UNQUALIFIED_DISTRIBUTOR,
) -> list[HealthFinding]:
    findings: list[HealthFinding] = []
    if any(
        command_uses_raw_tcp_registered_name(str(row.get("command") or ""))
        for row in job_commands
    ):
        target = port_qualified_distributor or unqualified_distributor
        findings.append(
            HealthFinding(
                "Error",
                f"Replication agent Publisher/Subscriber uses a raw tcp: endpoint. Recreate or patch it to the "
                f"logical registered replication names; Publisher target is {target}.",
                "replication agent endpoints",
            )
        )
    if any(
        command_uses_port_without_tcp(
            str(row.get("command") or ""),
            subsystem=str(row.get("subsystem") or ""),
        )
        for row in job_commands
    ):
        findings.append(
            HealthFinding(
                "Error",
                "Replication agent Distributor endpoint uses a port-qualified name without forcing TCP. "
                "Recreate or patch replication so Distributor uses a tcp: value from nodes.ini.",
                "replication agent endpoints",
            )
        )
    return findings


def current_repl_errors(
    rows: list[dict[str, Any]],
    repl_error_floor: datetime | None,
    *,
    logreader_history: list[dict[str, Any]] | None = None,
    distribution_history: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    history_rows = list(logreader_history or []) + list(distribution_history or [])
    failing_error_ids = latest_failing_history_error_ids(history_rows)
    healthy_floor = latest_healthy_history_time(history_rows)
    current = []
    for row in rows:
        error_time = row.get("time")
        error_id = row.get("id")
        try:
            error_id_int = int(error_id)
        except (TypeError, ValueError):
            error_id_int = 0
        if error_id_int in failing_error_ids:
            current.append(row)
            continue
        if repl_error_floor is not None and isinstance(error_time, datetime) and error_time < repl_error_floor:
            continue
        if repl_error_is_generic(row):
            continue
        if isinstance(error_time, datetime) and healthy_floor is not None and error_time <= healthy_floor:
            continue
        current.append(row)
    return current


def latest_history_by_agent(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    for row in rows:
        name = str(row.get("name") or "")
        if not name:
            continue
        if name not in latest:
            latest[name] = row
            continue
        existing_time = latest[name].get("time")
        row_time = row.get("time")
        if isinstance(row_time, datetime) and (not isinstance(existing_time, datetime) or row_time > existing_time):
            latest[name] = row
    return latest


def latest_failing_history_error_ids(rows: list[dict[str, Any]]) -> set[int]:
    error_ids: set[int] = set()
    for row in latest_history_by_agent(rows).values():
        if not history_row_is_warning(row):
            continue
        try:
            error_id = int(row.get("error_id") or 0)
        except (TypeError, ValueError):
            error_id = 0
        if error_id > 0:
            error_ids.add(error_id)
    return error_ids


def latest_healthy_history_time(rows: list[dict[str, Any]]) -> datetime | None:
    times = [
        row.get("time")
        for row in latest_history_by_agent(rows).values()
        if not history_row_is_warning(row) and isinstance(row.get("time"), datetime)
    ]
    return max(times) if times else None


def repl_error_is_generic(row: dict[str, Any]) -> bool:
    text = str(row.get("error_text") or "").lower()
    return any(
        token in text
        for token in (
            "last step did not log any message",
            "unspecified error",
        )
    )


def command_has_sql_agent_token(command: str) -> bool:
    return "$(" in str(command or "")


def command_uses_windows_auth(command: str) -> bool:
    return bool(re.search(r"-(Distributor|Publisher|Subscriber)SecurityMode\s+\[?1\]?", command, re.IGNORECASE))


def command_uses_port_without_tcp(command: str, *, subsystem: str = "") -> bool:
    for match in re.finditer(r"-(Publisher|Distributor|Subscriber)\s+\[?([^\]\s]+)\]?", command, re.IGNORECASE):
        label = match.group(1).lower()
        target = match.group(2)
        if label in {"publisher", "subscriber"}:
            continue
        if label == "distributor" and subsystem.lower() == "logreader":
            continue
        if "," in target and not target.lower().startswith("tcp:"):
            return True
    return False


def command_uses_raw_tcp_registered_name(command: str) -> bool:
    for match in re.finditer(r"-(Publisher|Subscriber)\s+\[?([^\]\s]+)\]?", command, re.IGNORECASE):
        if match.group(2).lower().startswith("tcp:"):
            return True
    return False


def agent_token_problem(message: str) -> bool:
    lower = str(message or "").lower()
    return "job step contains tokens" in lower or "escape_xxx" in lower


def agent_auth_problem(message: str) -> bool:
    lower = str(message or "").lower()
    return "cannot generate sspi context" in lower or "sspi" in lower


def latest_agent_history_warnings(rows: list[dict[str, Any]], name_key: str) -> list[dict[str, Any]]:
    warnings: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        name = str(row.get(name_key) or "")
        if not name or name in seen:
            continue
        seen.add(name)
        if history_row_is_warning(row):
            warnings.append(row)
    return warnings


def history_row_is_warning(row: dict[str, Any]) -> bool:
    try:
        runstatus = int(row.get("runstatus"))
    except (TypeError, ValueError):
        runstatus = 0
    comments = str(row.get("comments") or "").lower()
    return runstatus == 6 or any(token in comments for token in ("retry", "failed", "error"))


def benign_distribution_shutdown_history(row: dict[str, Any]) -> bool:
    try:
        runstatus = int(row.get("runstatus"))
    except (TypeError, ValueError):
        runstatus = 0
    try:
        error_id = int(row.get("error_id") or 0)
    except (TypeError, ValueError):
        error_id = 0
    comments = str(row.get("comments") or "").lower()
    return runstatus == 6 and error_id == 0 and "detect nonlogged agent shutdown" in comments


def format_health_finding(finding: HealthFinding) -> str:
    return f"{finding.severity}: {finding.message}"


def log_line_segments(timestamp: str, message: str) -> list[tuple[str, str]]:
    return [("log_timestamp", f"[{timestamp}] "), *log_message_segments(message), ("log_text", "\n")]


def log_message_segments(message: str) -> list[tuple[str, str]]:
    text = str(message)
    segments: list[tuple[str, str]] = []
    position = 0
    for match in LOG_TOKEN_RE.finditer(text):
        if match.start() > position:
            segments.append(("log_text", text[position : match.start()]))
        token = match.group(0)
        segments.append((log_tag_for_token(token), token))
        position = match.end()
    if position < len(text):
        segments.append(("log_text", text[position:]))
    return segments or [("log_text", "")]


def log_tag_for_token(token: str) -> str:
    normalized = token.lower()
    if normalized in {"o.k.", "ok"}:
        return "log_ok"
    if normalized in {"warning", "retry", "stopped"}:
        return "log_warn"
    if normalized in {"error", "failed", "refused", "cannot", "unable"}:
        return "log_error"
    return "log_action"


def read_current_replication_jobs(connection: Any, config: ReplConfig) -> tuple[ReplicationJobInfo, ...]:
    publisher = config.nodes[config.publisher]
    job_prefix = f"{publisher.expected_sql_name}-{config.database}-%"
    rows = all_rows(
        connection,
        """
        SELECT j.name, s.subsystem, COALESCE(l.id, d.id) AS agent_id
        FROM msdb.dbo.sysjobs AS j
        JOIN msdb.dbo.sysjobsteps AS s ON s.job_id = j.job_id
        LEFT JOIN distribution.dbo.MSlogreader_agents AS l
               ON l.name = j.name AND s.subsystem = N'LogReader'
        LEFT JOIN distribution.dbo.MSdistribution_agents AS d
               ON d.name = j.name AND s.subsystem = N'Distribution'
        WHERE j.name LIKE ?
          AND s.step_id = 2
          AND s.subsystem IN (N'LogReader', N'Distribution')
        ORDER BY CASE WHEN s.subsystem = N'LogReader' THEN 0 ELSE 1 END, j.name
        """,
        job_prefix,
    )
    jobs = tuple(
        ReplicationJobInfo(
            name=str(row.get("name") or ""),
            subsystem=str(row.get("subsystem") or ""),
            agent_id=int(row["agent_id"]) if row.get("agent_id") is not None else None,
        )
        for row in rows
        if row.get("name")
    )
    return jobs or tuple(ReplicationJobInfo(name=name) for name in START_JOB_ORDER)


def read_current_replication_job_names(connection: Any, config: ReplConfig) -> tuple[str, ...]:
    return tuple(job.name for job in read_current_replication_jobs(connection, config))


def normalize_replication_jobs(jobs: tuple[ReplicationJobInfo | str, ...]) -> tuple[ReplicationJobInfo, ...]:
    return tuple(job if isinstance(job, ReplicationJobInfo) else ReplicationJobInfo(name=str(job)) for job in jobs)


def read_job_statuses(connection: Any, jobs: tuple[ReplicationJobInfo | str, ...]) -> list[JobStatus]:
    job_infos = normalize_replication_jobs(jobs)
    job_names = tuple(job.name for job in job_infos)
    if not job_names:
        return []
    placeholders = ", ".join("?" for _ in job_names)
    rows = all_rows(
        connection,
        f"""
        SELECT j.name, c.name AS category, j.enabled,
               CASE WHEN ja.start_execution_date IS NOT NULL
                     AND ja.stop_execution_date IS NULL THEN 1 ELSE 0 END AS running,
               CONVERT(nvarchar(30), ja.start_execution_date, 120) AS start_execution_date,
               CONVERT(nvarchar(30), ja.stop_execution_date, 120) AS stop_execution_date
        FROM msdb.dbo.sysjobs AS j
        LEFT JOIN msdb.dbo.syscategories AS c ON c.category_id = j.category_id
        OUTER APPLY (
            SELECT TOP (1) start_execution_date, stop_execution_date
            FROM msdb.dbo.sysjobactivity AS a
            WHERE a.job_id = j.job_id
            ORDER BY a.session_id DESC
        ) AS ja
        WHERE j.name IN ({placeholders})
        ORDER BY j.name
        """,
        *job_names,
    )
    history = read_latest_job_history(connection, job_names)
    replication_statuses = read_distribution_replication_statuses(connection, job_infos)
    by_info = {job.name: job for job in job_infos}
    by_name: dict[str, JobStatus] = {}
    for row in rows:
        name = str(row["name"])
        hist = history.get(name, {})
        info = by_info.get(name, ReplicationJobInfo(name=name))
        replication_status = replication_statuses.get(name, {})
        by_name[name] = JobStatus(
            name=name,
            category=str(row.get("category") or ""),
            subsystem=info.subsystem,
            agent_id=info.agent_id,
            enabled=bool(row.get("enabled")),
            running=bool(row.get("running")),
            replication_running=replication_status.get("running"),
            replication_message=str(replication_status.get("message") or ""),
            start_execution_date=str(row.get("start_execution_date") or ""),
            stop_execution_date=str(row.get("stop_execution_date") or ""),
            last_run_status=int(hist["run_status"]) if "run_status" in hist and hist["run_status"] is not None else None,
            last_run_message=str(hist.get("message") or ""),
        )
    return [
        by_name.get(
            info.name,
            JobStatus(name=info.name, subsystem=info.subsystem, agent_id=info.agent_id),
        )
        for info in job_infos
    ]


def read_distribution_replication_statuses(connection: Any, jobs: tuple[ReplicationJobInfo, ...]) -> dict[str, dict[str, Any]]:
    distribution_jobs = [job for job in jobs if job.subsystem.lower() == "distribution" and job.agent_id is not None]
    if not distribution_jobs:
        return {}
    agent_ids = tuple(int(job.agent_id) for job in distribution_jobs if job.agent_id is not None)
    placeholders = ", ".join("?" for _ in agent_ids)
    try:
        subscription_rows = all_rows(
            connection,
            f"""
            SELECT agent_id,
                   COUNT(*) AS article_count,
                   SUM(CASE WHEN status = 2 THEN 1 ELSE 0 END) AS running_articles,
                   MIN(status) AS min_status,
                   MAX(status) AS max_status
            FROM distribution.dbo.MSsubscriptions
            WHERE agent_id IN ({placeholders})
            GROUP BY agent_id
            """,
            *agent_ids,
        )
        history_rows = all_rows(
            connection,
            f"""
            WITH latest_history AS (
                SELECT a.id AS agent_id, h.time, h.runstatus, h.error_id, h.comments,
                       ROW_NUMBER() OVER (PARTITION BY a.id ORDER BY h.time DESC) AS row_number
                FROM distribution.dbo.MSdistribution_agents AS a
                LEFT JOIN distribution.dbo.MSdistribution_history AS h ON h.agent_id = a.id
                WHERE a.id IN ({placeholders})
            )
            SELECT agent_id, time, runstatus, error_id, comments
            FROM latest_history
            WHERE row_number = 1
            """,
            *agent_ids,
        )
    except Exception:
        return {}

    subscriptions_by_agent = {int(row["agent_id"]): row for row in subscription_rows if row.get("agent_id") is not None}
    history_by_agent = {int(row["agent_id"]): row for row in history_rows if row.get("agent_id") is not None}
    statuses: dict[str, dict[str, Any]] = {}
    for job in distribution_jobs:
        assert job.agent_id is not None
        subscription = subscriptions_by_agent.get(job.agent_id, {})
        history = history_by_agent.get(job.agent_id, {})
        article_count = int(subscription.get("article_count") or 0)
        running_articles = int(subscription.get("running_articles") or 0)
        comments = str(history.get("comments") or "")
        subscription_running = article_count >= len(WGD_TABLES) and running_articles == article_count
        latest_history_ok = (
            not comments
            or not history_row_is_warning(history)
            or subscription_running
            and benign_distribution_shutdown_history(history)
        )
        running = subscription_running and latest_history_ok
        if running and benign_distribution_shutdown_history(history):
            message = f"{running_articles}/{article_count} subscription articles active"
        elif comments:
            message = comments
        elif article_count:
            message = f"{running_articles}/{article_count} articles active"
        else:
            message = "No distribution subscription rows found."
        statuses[job.name] = {"running": running, "message": message}
    return statuses


def read_latest_job_history(connection: Any, job_names: tuple[str, ...]) -> dict[str, dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    for job_name in job_names:
        rows = all_rows(
            connection,
            """
            SELECT TOP (1) h.run_status, h.message
            FROM msdb.dbo.sysjobhistory AS h
            JOIN msdb.dbo.sysjobs AS j ON j.job_id = h.job_id
            WHERE j.name = ?
            ORDER BY h.instance_id DESC
            """,
            job_name,
        )
        latest[job_name] = rows[0] if rows else {}
    return latest


def read_publisher_host_info(connection: Any) -> dict[str, Any]:
    rows = all_rows(
        connection,
        """
        SELECT @@SERVERNAME AS sql_name,
               CAST(SERVERPROPERTY('MachineName') AS nvarchar(256)) AS machine_name,
               host_platform, host_distribution, host_release
        FROM sys.dm_os_host_info
        """,
    )
    return rows[0] if rows else {}


def read_registered_servers(connection: Any) -> list[dict[str, Any]]:
    return all_rows(
        connection,
        """
        SELECT server_id, name, data_source, provider, provider_string, is_linked, is_remote_login_enabled
        FROM sys.servers
        ORDER BY server_id
        """,
    )


def read_replication_job_commands(connection: Any, job_names: tuple[str, ...]) -> list[dict[str, Any]]:
    if not job_names:
        return []
    placeholders = ", ".join("?" for _ in job_names)
    return all_rows(
        connection,
        f"""
        SELECT j.name, s.step_id, s.subsystem, s.command
        FROM msdb.dbo.sysjobs AS j
        JOIN msdb.dbo.sysjobsteps AS s ON s.job_id = j.job_id
        WHERE j.name IN ({placeholders}) AND s.step_id = 2
        ORDER BY j.name
        """,
        *job_names,
    )


def read_job_history_details(connection: Any, job_names: tuple[str, ...], *, limit: int = 80) -> dict[str, list[dict[str, Any]]]:
    details: dict[str, list[dict[str, Any]]] = {}
    for job_name in job_names:
        details[job_name] = all_rows(
            connection,
            f"""
            SELECT TOP ({int(limit)}) h.instance_id, h.step_id, h.run_status, h.retries_attempted,
                   CONVERT(nvarchar(30), msdb.dbo.agent_datetime(h.run_date, h.run_time), 120) AS history_time,
                   h.message
            FROM msdb.dbo.sysjobhistory AS h
            JOIN msdb.dbo.sysjobs AS j ON j.job_id = h.job_id
            WHERE j.name = ? AND h.step_id = 2
            ORDER BY h.instance_id DESC
            """,
            job_name,
        )
    return details


def current_agent_ids(jobs: list[JobStatus], subsystem: str) -> tuple[int, ...]:
    return tuple(
        int(job.agent_id)
        for job in jobs
        if job.agent_id is not None and job.subsystem.lower() == subsystem.lower()
    )


def read_current_agent_generation_floor(
    connection: Any,
    *,
    logreader_agent_ids: tuple[int, ...],
    distribution_agent_ids: tuple[int, ...],
) -> datetime | None:
    starts: list[datetime] = []
    if logreader_agent_ids:
        placeholders = ", ".join("?" for _ in logreader_agent_ids)
        rows = all_rows(
            connection,
            f"""
            SELECT agent_id, MAX(time) AS start_time
            FROM dbo.MSlogreader_history
            WHERE agent_id IN ({placeholders}) AND comments LIKE N'Starting agent%'
            GROUP BY agent_id
            """,
            *logreader_agent_ids,
        )
        starts.extend(row["start_time"] for row in rows if isinstance(row.get("start_time"), datetime))
    if distribution_agent_ids:
        placeholders = ", ".join("?" for _ in distribution_agent_ids)
        rows = all_rows(
            connection,
            f"""
            SELECT agent_id, MAX(time) AS start_time
            FROM dbo.MSdistribution_history
            WHERE agent_id IN ({placeholders}) AND comments LIKE N'Starting agent%'
            GROUP BY agent_id
            """,
            *distribution_agent_ids,
        )
        starts.extend(row["start_time"] for row in rows if isinstance(row.get("start_time"), datetime))
    return min(starts) if starts else None


def read_logreader_history(connection: Any, *, agent_ids: tuple[int, ...] = (), limit: int = 20) -> list[dict[str, Any]]:
    where = ""
    params: tuple[object, ...] = ()
    if agent_ids:
        placeholders = ", ".join("?" for _ in agent_ids)
        where = f"WHERE a.id IN ({placeholders})"
        params = tuple(agent_ids)
    return all_rows(
        connection,
        f"""
        SELECT TOP ({int(limit)}) a.name, h.time, h.runstatus, h.duration, h.comments, h.error_id
        FROM dbo.MSlogreader_agents AS a
        JOIN dbo.MSlogreader_history AS h ON h.agent_id = a.id
        {where}
        ORDER BY h.time DESC
        """,
        *params,
    )


def read_distribution_history(connection: Any, *, agent_ids: tuple[int, ...] = (), limit: int = 30) -> list[dict[str, Any]]:
    where = ""
    params: tuple[object, ...] = ()
    if agent_ids:
        placeholders = ", ".join("?" for _ in agent_ids)
        where = f"WHERE a.id IN ({placeholders})"
        params = tuple(agent_ids)
    return all_rows(
        connection,
        f"""
        SELECT TOP ({int(limit)}) a.name, a.id AS agent_id, a.subscriber_name, a.subscriber_db,
               h.time, h.runstatus, h.duration, h.comments, h.error_id
        FROM dbo.MSdistribution_agents AS a
        JOIN dbo.MSdistribution_history AS h ON h.agent_id = a.id
        {where}
        ORDER BY h.time DESC
        """,
        *params,
    )


def read_repl_errors(connection: Any, *, after: datetime | None = None, limit: int = 20) -> list[dict[str, Any]]:
    where = "WHERE time >= ?" if after is not None else ""
    params: tuple[object, ...] = (after,) if after is not None else ()
    return all_rows(
        connection,
        f"""
        SELECT TOP ({int(limit)}) id, time, source_name, error_code, error_text, session_id
        FROM dbo.MSrepl_errors
        {where}
        ORDER BY time DESC, id DESC
        """,
        *params,
    )


def execute_job_command(connection: Any, command: str, job_names: tuple[str, ...]) -> list[JobActionResult]:
    proc = "sp_start_job" if command == "start" else "sp_stop_job"
    results: list[JobActionResult] = []
    cursor = connection.cursor()
    try:
        for job_name in job_names:
            try:
                cursor.execute(f"EXEC msdb.dbo.{proc} @job_name = ?", job_name)
                results.append(JobActionResult(job_name=job_name, command=command, ok=True))
            except Exception as exc:
                results.append(JobActionResult(job_name=job_name, command=command, ok=False, error=str(exc)))
    finally:
        cursor.close()
    return results


class DarkClusterWatch(tk.Tk):
    def __init__(self, service: ClusterService, initial_interval: int = 30) -> None:
        super().__init__()
        self.service = service
        self.interval_seconds = tk.IntVar(value=clamp_interval(initial_interval))
        self.busy = False
        self.refresh_after_id: str | None = None
        self.node_fields: dict[str, dict[str, tk.Label]] = {}
        self.job_rows: dict[str, str] = {}

        self.title(APP_TITLE)
        self.configure(bg=COLORS["bg"])
        self.geometry("1160x760")
        self.minsize(980, 650)
        self._configure_style()
        self._build_ui()
        self.protocol("WM_DELETE_WINDOW", self._on_close)
        self._run_worker("Initial refresh", self.service.refresh, self._apply_snapshot)

    def _configure_style(self) -> None:
        style = ttk.Style(self)
        style.theme_use("clam")
        style.configure("Dark.TFrame", background=COLORS["bg"])
        style.configure("Panel.TFrame", background=COLORS["panel"], bordercolor=COLORS["border"], relief="solid")
        style.configure("TLabel", background=COLORS["bg"], foreground=COLORS["text"])
        style.configure("Muted.TLabel", background=COLORS["bg"], foreground=COLORS["muted"])
        style.configure("Title.TLabel", background=COLORS["bg"], foreground=COLORS["text"], font=("Segoe UI", 20, "bold"))
        style.configure("PanelTitle.TLabel", background=COLORS["panel"], foreground=COLORS["text"], font=("Segoe UI", 12, "bold"))
        style.configure("PanelText.TLabel", background=COLORS["panel"], foreground=COLORS["text"])
        style.configure("PanelMuted.TLabel", background=COLORS["panel"], foreground=COLORS["muted"])
        style.configure("Dark.TButton", background=COLORS["button"], foreground=COLORS["text"], bordercolor=COLORS["border"], focusthickness=0)
        style.map("Dark.TButton", background=[("active", COLORS["button_active"]), ("disabled", COLORS["panel"])])
        style.configure("Dark.Treeview", background=COLORS["panel"], fieldbackground=COLORS["panel"], foreground=COLORS["text"], bordercolor=COLORS["border"])
        style.configure("Dark.Treeview.Heading", background=COLORS["panel_alt"], foreground=COLORS["text"], relief="flat")
        style.map("Dark.Treeview", background=[("selected", COLORS["button_active"])])

    def _build_ui(self) -> None:
        root = ttk.Frame(self, style="Dark.TFrame", padding=16)
        root.pack(fill="both", expand=True)

        header = ttk.Frame(root, style="Dark.TFrame")
        header.pack(fill="x")
        ttk.Label(header, text=APP_TITLE, style="Title.TLabel").pack(side="left")

        header_status = ttk.Frame(header, style="Dark.TFrame")
        header_status.pack(side="right")
        self.operation_label = ttk.Label(header_status, text="Idle", style="Muted.TLabel")
        self.operation_label.pack(side="left", padx=(0, 16))
        self.connectivity_label = ttk.Label(header_status, text="Connectivity: ...", style="Muted.TLabel")
        self.connectivity_label.pack(side="left", padx=(0, 16))
        self.sync_label = ttk.Label(header_status, text="Sync: ...", style="Muted.TLabel")
        self.sync_label.pack(side="left")

        controls = ttk.Frame(root, style="Dark.TFrame")
        controls.pack(fill="x", pady=(14, 12))
        self.refresh_button = ttk.Button(controls, text="Refresh now", style="Dark.TButton", command=self.refresh_now)
        self.health_button = ttk.Button(controls, text="Health check", style="Dark.TButton", command=self.health_check)
        self.start_button = ttk.Button(controls, text="Start replication", style="Dark.TButton", command=self.start_replication)
        self.stop_button = ttk.Button(controls, text="Stop replication", style="Dark.TButton", command=self.stop_replication)
        self.restart_button = ttk.Button(controls, text="Restart replication", style="Dark.TButton", command=self.restart_replication)
        for button in (self.refresh_button, self.health_button, self.start_button, self.stop_button, self.restart_button):
            button.pack(side="left", padx=(0, 8))

        ttk.Label(controls, text="Refresh interval", style="Muted.TLabel").pack(side="left", padx=(18, 6))
        self.interval_spin = tk.Spinbox(
            controls,
            from_=MIN_REFRESH_SECONDS,
            to=MAX_REFRESH_SECONDS,
            increment=5,
            width=6,
            textvariable=self.interval_seconds,
            command=self._interval_changed,
            bg=COLORS["panel"],
            fg=COLORS["text"],
            buttonbackground=COLORS["button"],
            insertbackground=COLORS["text"],
            highlightbackground=COLORS["border"],
            relief="flat",
        )
        self.interval_spin.pack(side="left")
        ttk.Label(controls, text="sec", style="Muted.TLabel").pack(side="left", padx=(4, 10))
        self.interval_scale = tk.Scale(
            controls,
            from_=MIN_REFRESH_SECONDS,
            to=MAX_REFRESH_SECONDS,
            orient="horizontal",
            showvalue=False,
            length=220,
            variable=self.interval_seconds,
            command=lambda _value: self._interval_changed(),
            bg=COLORS["bg"],
            fg=COLORS["text"],
            troughcolor=COLORS["panel_alt"],
            activebackground=COLORS["accent"],
            highlightthickness=0,
        )
        self.interval_scale.pack(side="left")

        nodes_frame = ttk.Frame(root, style="Dark.TFrame")
        nodes_frame.pack(fill="x", pady=(4, 14))
        for node_name in ("NODE010", "NODE020", "NODE030"):
            self._build_node_panel(nodes_frame, node_name)

        ttk.Label(root, text="Replication Jobs", style="Muted.TLabel").pack(anchor="w")
        self.job_tree = ttk.Treeview(
            root,
            columns=("state", "enabled", "started", "message"),
            show="tree headings",
            height=6,
            style="Dark.Treeview",
        )
        self.job_tree.heading("#0", text="Job")
        self.job_tree.heading("state", text="State")
        self.job_tree.heading("enabled", text="Enabled")
        self.job_tree.heading("started", text="Started")
        self.job_tree.heading("message", text="Last message")
        self.job_tree.column("#0", width=350, stretch=False)
        self.job_tree.column("state", width=110, stretch=False)
        self.job_tree.column("enabled", width=80, stretch=False)
        self.job_tree.column("started", width=160, stretch=False)
        self.job_tree.column("message", width=430, stretch=True)
        self.job_tree.pack(fill="x", pady=(6, 14))

        ttk.Label(root, text="Activity", style="Muted.TLabel").pack(anchor="w")
        self.log_text = tk.Text(
            root,
            height=8,
            bg="#0c1014",
            fg=LOG_COLORS["log_text"],
            insertbackground=COLORS["text"],
            relief="flat",
            wrap="word",
        )
        self.log_text.pack(fill="both", expand=True, pady=(6, 0))
        self._configure_log_tags()
        self.log_text.configure(state="disabled")

    def _build_node_panel(self, parent: ttk.Frame, node_name: str) -> None:
        panel = ttk.Frame(parent, style="Panel.TFrame", padding=12)
        panel.pack(side="left", fill="both", expand=True, padx=(0, 10))
        ttk.Label(panel, text=node_name, style="PanelTitle.TLabel").pack(anchor="w")
        fields: dict[str, tk.Label] = {}
        for label in ("Status", "Server", "SQL name", "Edition", "POVIID", "WGD tables", "Rows"):
            row = ttk.Frame(panel, style="Panel.TFrame")
            row.pack(fill="x", pady=(6, 0))
            ttk.Label(row, text=label, style="PanelMuted.TLabel", width=10).pack(side="left")
            value = tk.Label(row, text="...", bg=COLORS["panel"], fg=COLORS["text"], anchor="w")
            value.pack(side="left", fill="x", expand=True)
            fields[label] = value
        self.node_fields[node_name] = fields

    def _interval_changed(self) -> None:
        self.interval_seconds.set(clamp_interval(self.interval_seconds.get()))
        self._schedule_refresh(reset=True)

    def refresh_now(self) -> None:
        self._run_worker("Refresh", self.service.refresh, self._apply_snapshot)

    def health_check(self) -> None:
        self._run_worker("Health check", self.service.health_check, self._apply_health_findings)

    def start_replication(self) -> None:
        self._run_worker("Start replication", self.service.start_replication, self._apply_action_results)

    def stop_replication(self) -> None:
        self._run_worker("Stop replication", self.service.stop_replication, self._apply_action_results)

    def restart_replication(self) -> None:
        self._run_worker("Restart replication", self.service.restart_replication, self._apply_action_results)

    def _run_worker(self, label: str, worker: Callable[[], Any], callback: Callable[[Any], None]) -> None:
        if self.busy:
            return
        self.busy = True
        self._set_controls_enabled(False)
        self.operation_label.configure(text=f"{label}...")
        self._log(f"{label} started")

        def run() -> None:
            try:
                result = worker()
                self.after(0, lambda: self._worker_done(label, result, callback, None))
            except Exception as exc:
                self.after(0, lambda: self._worker_done(label, None, callback, exc))

        threading.Thread(target=run, daemon=True).start()

    def _worker_done(self, label: str, result: Any, callback: Callable[[Any], None], error: Exception | None) -> None:
        self.busy = False
        self._set_controls_enabled(True)
        if error:
            self.operation_label.configure(text=f"{label} failed")
            self._log(f"{label} failed: {error}")
            self._schedule_refresh(reset=True)
            return
        callback(result)
        self.operation_label.configure(text=f"{label} done")
        self._schedule_refresh(reset=True)

    def _set_controls_enabled(self, enabled: bool) -> None:
        state = "normal" if enabled else "disabled"
        for button in (self.refresh_button, self.health_button, self.start_button, self.stop_button, self.restart_button):
            button.configure(state=state)

    def _apply_snapshot(self, snapshot: ClusterSnapshot) -> None:
        for status in snapshot.nodes:
            self._update_node(status)
        self._update_jobs(snapshot.jobs)
        self._update_health(snapshot)
        for error in snapshot.errors:
            self._log(error)
        if last_log := important_job_log(snapshot.jobs):
            self._log(last_log)
        self._log(f"Snapshot {snapshot.generated_at:%H:%M:%S}")

    def _update_health(self, snapshot: ClusterSnapshot) -> None:
        connectivity = connectivity_indicator(snapshot.nodes, snapshot.errors)
        sync = sync_indicator(snapshot.jobs, snapshot.errors)
        self.connectivity_label.configure(text=f"Connectivity: {connectivity.label}", foreground=connectivity.color)
        self.sync_label.configure(text=f"Sync: {sync.label}", foreground=sync.color)

    def _update_node(self, status: NodeStatus) -> None:
        fields = self.node_fields.get(status.name)
        if not fields:
            return
        indicator = node_status_indicator(status)
        fields["Status"].configure(text=indicator.label, fg=indicator.color)
        if status.connected:
            fields["SQL name"].configure(text=f"{status.sql_name} / {status.machine_name}")
            fields["Edition"].configure(text=status.edition)
            fields["POVIID"].configure(text=f"{status.database_state} / {status.recovery_model}")
            fields["WGD tables"].configure(text=f"{status.wgd_table_count}/{len(WGD_TABLES)}")
            fields["Rows"].configure(text=f"{status.wgd_total_rows:,}")
        else:
            fields["SQL name"].configure(text=status.error[:90])
            fields["Edition"].configure(text="")
            fields["POVIID"].configure(text="")
            fields["WGD tables"].configure(text="")
            fields["Rows"].configure(text="")
        fields["Server"].configure(text=status.server)

    def _update_jobs(self, jobs: list[JobStatus]) -> None:
        present = set()
        for job in jobs:
            state, color = job_state_label(job)
            present.add(job.name)
            message = job_display_message(job)
            values = (state, "yes" if job.enabled else "no", job.start_execution_date, message)
            if job.name not in self.job_rows:
                item_id = self.job_tree.insert("", "end", text=job.name, values=values)
                self.job_rows[job.name] = item_id
            else:
                item_id = self.job_rows[job.name]
                self.job_tree.item(item_id, values=values)
            tag = f"state_{state}"
            self.job_tree.item(item_id, tags=(tag,))
            self.job_tree.tag_configure(tag, foreground=color)
        for job_name, item_id in list(self.job_rows.items()):
            if job_name not in present:
                self.job_tree.delete(item_id)
                del self.job_rows[job_name]

    def _apply_action_results(self, results: list[JobActionResult]) -> None:
        for result in results:
            if result.ok:
                self._log(f"{result.command} {result.job_name}: ok")
            else:
                self._log(f"{result.command} {result.job_name}: {result.error}")
        self.after(100, self.refresh_now)

    def _apply_health_findings(self, findings: list[HealthFinding]) -> None:
        for finding in findings:
            self._log(format_health_finding(finding))

    def _schedule_refresh(self, *, reset: bool = False) -> None:
        if self.refresh_after_id is not None and reset:
            self.after_cancel(self.refresh_after_id)
            self.refresh_after_id = None
        if self.refresh_after_id is None:
            self.refresh_after_id = self.after(clamp_interval(self.interval_seconds.get()) * 1000, self._scheduled_refresh)

    def _scheduled_refresh(self) -> None:
        self.refresh_after_id = None
        if not self.busy:
            self.refresh_now()
        else:
            self._schedule_refresh(reset=True)

    def _configure_log_tags(self) -> None:
        for tag_name, color in LOG_COLORS.items():
            self.log_text.tag_configure(tag_name, foreground=color)

    def _log(self, message: str) -> None:
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.log_text.configure(state="normal")
        for tag_name, text in log_line_segments(timestamp, message):
            self.log_text.insert("end", text, tag_name)
        self.log_text.see("end")
        self.log_text.configure(state="disabled")

    def _on_close(self) -> None:
        if self.refresh_after_id is not None:
            self.after_cancel(self.refresh_after_id)
        self.destroy()


def compact_message(message: str, limit: int = 180) -> str:
    compact = " ".join(str(message or "").split())
    if len(compact) > limit:
        return compact[: limit - 3] + "..."
    return compact


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Dark Tkinter monitor for WGD SQL Server replication.")
    parser.add_argument("--version", action="version", version=APP_TITLE)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG, help=f"node config path (default: {DEFAULT_CONFIG})")
    parser.add_argument("--interval", type=int, default=30, help="refresh interval in seconds, clamped to 5-900")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    service = ClusterService(args.config)
    app = DarkClusterWatch(service, initial_interval=args.interval)
    app.mainloop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
