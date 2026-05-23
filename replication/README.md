# WGD SQL Server Lab Replication

This folder contains operator tooling for the lab topology:

- `NODE010`: SQL Server Developer publisher/distributor.
- `NODE020`: SQL Server Express read-only subscriber at `192.168.1.7,4002`.
- `NODE030`: SQL Server Express read-only subscriber at `192.168.1.19,4003`.

The tools do not store the `sa` password. They read it from `/tmp/mssqlpass` by
default.

## 1. Preflight

```bash
python3 wgd/replication/wgd_replication.py --config wgd/replication/nodes.ini.example preflight
```

The current lab check found two blockers that must be cleared before live
replication setup:

- `NODE020` is failing the SQL Server pre-login handshake.
- SQL Server logical names appear cloned; replication should use unique logical
  names for the three instances.

## 2. Backups

After connectivity is clean, create copy-only backups on each SQL Server host:

```bash
python3 wgd/replication/wgd_replication.py --config wgd/replication/nodes.ini.example backup
```

Backups are written to each instance's default backup path.

## 3. Fix SQL Server Logical Names

Generate the per-node name repair script:

```bash
python3 wgd/replication/wgd_replication.py --config wgd/replication/nodes.ini.example generate-name-fix-sql > /tmp/wgd_fix_names.sql
```

Run each block on the matching node, restart that SQL Server instance, then run
preflight again. Continue only when every node has a unique `@@SERVERNAME`.

## 4. Create Or Recreate Replication

Generate the replication setup script after server names and connectivity are
fixed:

```bash
python3 wgd/replication/wgd_replication.py --config wgd/replication/nodes.ini.example create --password 'YOUR_SQL_PASSWORD' > /tmp/wgd_replication_create.sql
```

Run the generated script against `NODE010`. It configures local distribution,
creates the transactional publication, adds the WGD `tmp_*` articles, and adds
push subscriptions initialized with existing data. The generated SQL embeds the
given password in the agent commands, normalizes them for SQL Authentication,
uses registered logical names for `-Publisher` and `-Subscriber`, and forces
the Distributor connection through the explicit `tcp:` endpoint from
`nodes.ini`. Subscriber network ports are resolved through the registered
server metadata on `NODE010`.
Because this WSL/Linux topology includes non-default ports, generated replication
names include ports: `NODE010,4001`, `NODE020,4002`, and `NODE030,4003`.

If old replication metadata already exists, recreate it instead:

```bash
python3 wgd/replication/wgd_replication.py --config wgd/replication/nodes.ini.example recreate --password 'YOUR_SQL_PASSWORD' > /tmp/wgd_replication_recreate.sql
```

The recreate script removes and recreates replication metadata/jobs only; it
leaves existing WGD tables and rows in place.

If replication metadata is healthy but SQL Agent job commands drift, patch only
the existing agent jobs:

```bash
python3 wgd/replication/wgd_replication.py --config wgd/replication/nodes.ini.example \
  generate-agent-fix-sql > /tmp/wgd_replication_agent_fix.sql
```

Run the generated script against `NODE010`. It stops the replication agents,
patches job commands, then starts the agents again. It does not recreate
publications, subscriptions, articles, or WGD data.

When the Distribution Agent uses logical Subscriber names such as `NODE020` but
the Subscribers listen on non-default ports, create SQL client aliases on the
`NODE010` SQL Agent host:

```bash
python3 wgd/replication/wgd_replication.py --config wgd/replication/nodes.ini.example \
  generate-client-alias-fix > /tmp/wgd_client_alias_fix.ps1
```

Run the generated PowerShell as Administrator on `NODE010`, then restart only the
Distribution Agent jobs. This preserves existing replication metadata and
pending distribution commands.

The SQL Server Agent runtime should also be able to resolve all names:

```text
192.168.1.2    NODE010
192.168.1.7    NODE020
192.168.1.19   NODE030
```

## 5. Important Loader Rule

Do not use the existing `mssqlldr/loader.sh` full `TRUNCATE`/reload workflow
during normal replication. That would produce a huge replicated delete/insert
storm over the 500 kbit/s link.

## Dark Cluster Watch

Launch the dark Tkinter monitor from the repository root:

```bash
python3 wgd/replication/dark_cluster_watch.py
```

It displays NODE010/NODE020/NODE030 health, replication job state, latest SQL
Agent history, and provides Start, Stop, Restart, and Refresh controls. The
refresh interval is clamped to 5-900 seconds.
