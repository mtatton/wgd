# WGD MSSQL Connection

Read-only connection and smoke-query tooling for the World Graph Database
loaded into Microsoft SQL Server.

## Setup

Create a local config from the example:

```bash
cp wgd/connection/config.ini.example wgd/connection/config.ini
```

Edit `wgd/connection/config.ini` if the username, driver, or TLS settings differ
on the target SQL Server. Do not put the password in the config file; keep using
the mssqltop-style password environment variable:

```bash
export MSSQLTOP='your-password'
```

## Run The Graph Vector Query Test

```bash
python3 wgd/connection/query_graph_vector_db.py --config wgd/connection/config.ini
```

The script reuses the existing `mssqlt/mssqltop.py` connection helpers. It
checks for the expected graph/vector tables, preferring the `tmp_` table prefix
used by `mssqlldr/loader.sh` and falling back to unprefixed tables. It then
prints row counts plus a small sample from the vector-bearing graph tables.

JSON output is available for automation:

```bash
python3 wgd/connection/query_graph_vector_db.py --format json
```

The query test is read-only. It does not create, truncate, load, or update any
SQL Server tables.

## Run The Scene Cluster Query Test

Use the scene-construction query as a publisher-only smoke test:

```bash
python3 wgd/connection/query_scene_cluster.py --format text
```

Use the WGD cluster explicitly to inspect the publisher and both subscribers at
the same time:

```bash
python3 wgd/connection/query_scene_cluster.py --use-cluster --format text
```

Inspect a specific canonical scene id or scene key across the cluster and emit
JSON:

```bash
python3 wgd/connection/query_scene_cluster.py --use-cluster --scene-id 00934 --format json
```

The query reads `wgd/replication/nodes.ini` and uses `/tmp/mssqlpass` through
the replication helper. Publisher-only mode connects to `NODE010` with one
worker. Cluster mode connects to `NODE010`, `NODE020`, and `NODE030`
concurrently with three workers. It only runs `SELECT` statements against the
WGD `tmp_*` tables.
