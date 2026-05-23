# Using WGD In Scene Construction

WGD is the scene memory layer for POV construction work. Use it to retrieve
existing scenes, objects, materials, cameras, lights, descriptions, graph
regions, and source-code context before composing or editing a scene.

The working sources live in `vector_databases/*.sqlite`. The SQL Server cluster
uses the same logical tables with a `tmp_` prefix, synchronized by
`wgd/sync/wgd_sqlite_sync.py`.

## Construction Principle

Start from evidence, then compose. A new scene should be grounded in WGD rows
that describe:

- visual intent from `scenes` and `descriptions`;
- reusable geometry or crops from `objects`;
- POV source structure from `povs`;
- camera placement from `cameras`;
- lighting style from `lights`;
- surface language from `materials`;
- spatial adjacency from `spatial_nodes`, `spatial_edges`, and `spatial_cells`;
- higher-level motifs from `graph_clusters` and `graph_cluster_members`.

Do not treat a single nearest neighbor as the whole answer. Prefer a small
bundle of compatible evidence: one scene anchor, one or more object anchors, one
camera, one lighting pattern, one material family, and any relevant graph
cluster or spatial cell.

## Local Tables

The local databases are the fastest place to inspect construction ingredients:

| Database | Table | Use in construction |
| --- | --- | --- |
| `scene_vectors.sqlite` | `scenes` | Render-level visual anchors, palette, brightness, contrast, image size, canonical scene id. |
| `description_vectors.sqlite` | `descriptions` | Human-readable title and summary for scene intent. |
| `object_vectors.sqlite` | `objects` | Salient crops, bounding boxes, palette, crop role, and scene membership. |
| `pov_vectors.sqlite` | `povs` | Full POV file statistics, source path, primitive/material/light/camera counts. |
| `material_vectors.sqlite` | `materials` | Material snippets, token features, color/texture/finish/reflection/transparency signals. |
| `camera_vectors.sqlite` | `cameras` | Camera snippets, location/look_at/angle text, derived height, distance, yaw, and pitch. |
| `light_vectors.sqlite` | `lights` | Light snippets, type, role, color family, numeric position, spotlight/area/shadow traits. |
| `space_graph.sqlite` | `spatial_nodes` | Typed graph nodes with scene ids, labels, source links, optional 3D bounds, and vectors. |
| `space_graph.sqlite` | `spatial_edges` | Relationships between nodes, including edge type, weight, and distance. |
| `space_graph.sqlite` | `spatial_cells` | Scene-local spatial bins with member counts and retrieval text. |
| `space_graph.sqlite` | `graph_clusters` | Motifs or regions with summaries, primary nodes, confidence, and retrieval text. |
| `space_graph.sqlite` | `graph_cluster_members` | Cluster-to-node membership with role, weight, and reason. |
| `meta_source_graph.sqlite` | `source_*` | Source-file, symbol, chunk, and dependency context when code provenance matters. |

## Scene Construction Workflow

1. Define the brief.

   Write one or two sentences describing the target mood, subject, spatial
   layout, and constraints. Include whether the output is a new `.pov` file, a
   modification of an existing scene, or a prompt/specification for later work.

2. Select a scene anchor.

   Use `scenes` and `descriptions` to find a canonical scene with compatible
   visual intent. Check palette, brightness, contrast, source path, and summary
   before copying any source pattern.

3. Gather construction parts.

   Pull candidate objects, materials, camera blocks, and light blocks from the
   same `canonical_scene_id` when cohesion matters. When remixing, combine
   anchors from different scenes only after checking that scale, palette,
   camera angle, and light role are compatible.

4. Read the graph before writing.

   Use `spatial_nodes` for entities and source links, `spatial_edges` for
   adjacency, and `spatial_cells` or `graph_clusters` for region-level
   structure. This prevents scenes that have nice fragments but incoherent
   layout.

5. Compose the scene.

   Create or edit POV source using the selected evidence as references. Keep
   camera, light, and material snippets recognizable enough to preserve proven
   behavior, but normalize names and transforms for the target scene.

6. Render and inspect.

   Compare the render against the brief and the retrieved anchors. If the result
   diverges, adjust camera, light intensity, material finish, and object scale
   before changing the whole structure.

7. Re-index and sync only after the scene is accepted.

   Once local vector databases are updated by the indexing pipeline, use the WGD
   sync tool to publish changes. Avoid full truncate/reload workflows during
   replication.

## Useful Local Queries

List scenes with compact visual metadata:

```bash
sqlite3 vector_databases/scene_vectors.sqlite "
SELECT scene_key, canonical_scene_id, source_path, width, height,
       round(brightness, 3) AS brightness,
       round(contrast, 3) AS contrast,
       substr(palette_json, 1, 120) AS palette
FROM scenes
ORDER BY scene_key
LIMIT 20;"
```

Find the description for a canonical scene:

```bash
sqlite3 vector_databases/description_vectors.sqlite "
SELECT canonical_scene_id, title, summary
FROM descriptions
WHERE canonical_scene_id = 'SCENE_ID_HERE';"
```

Inspect objects in a scene by saliency:

```bash
sqlite3 vector_databases/object_vectors.sqlite "
SELECT object_key, source_path, crop_role,
       round(area_ratio, 3) AS area_ratio,
       round(saliency_score, 3) AS saliency,
       bbox_x, bbox_y, bbox_width, bbox_height
FROM objects
WHERE canonical_scene_id = 'SCENE_ID_HERE'
ORDER BY saliency_score DESC, area_ratio DESC
LIMIT 20;"
```

Inspect camera candidates:

```bash
sqlite3 vector_databases/camera_vectors.sqlite "
SELECT camera_key, source_path, camera_name,
       location_text, look_at_text, angle_text,
       round(camera_height, 3) AS height,
       round(camera_distance, 3) AS distance,
       round(camera_yaw, 3) AS yaw,
       round(camera_pitch, 3) AS pitch,
       snippet
FROM cameras
WHERE canonical_scene_id = 'SCENE_ID_HERE'
ORDER BY has_numeric_camera DESC, camera_key
LIMIT 10;"
```

Inspect light candidates:

```bash
sqlite3 vector_databases/light_vectors.sqlite "
SELECT light_key, source_path, light_type, light_role, color_family,
       round(light_x, 3) AS x,
       round(light_y, 3) AS y,
       round(light_z, 3) AS z,
       round(color_intensity, 3) AS intensity,
       has_area_light, has_spotlight, has_shadowless,
       snippet
FROM lights
WHERE canonical_scene_id = 'SCENE_ID_HERE'
ORDER BY light_role, light_key
LIMIT 20;"
```

Inspect material candidates:

```bash
sqlite3 vector_databases/material_vectors.sqlite "
SELECT material_key, source_path, material_name, block_type, material_family,
       color_count, has_texture, has_pigment, has_finish,
       has_reflection, has_transparency,
       snippet
FROM materials
WHERE canonical_scene_id = 'SCENE_ID_HERE'
ORDER BY material_family, material_key
LIMIT 20;"
```

Read the spatial graph for a scene:

```bash
sqlite3 vector_databases/space_graph.sqlite "
SELECT node_key, node_type, label, source_table, source_key,
       x_min, x_max, y_min, y_max, z_min, z_max,
       round(confidence, 3) AS confidence
FROM spatial_nodes
WHERE canonical_scene_id = 'SCENE_ID_HERE'
ORDER BY node_type, label
LIMIT 50;"
```

Read relationships around one node:

```bash
sqlite3 vector_databases/space_graph.sqlite "
SELECT from_node_key, edge_type, to_node_key,
       round(weight, 3) AS weight,
       round(distance, 3) AS distance,
       metadata_json
FROM spatial_edges
WHERE from_node_key = 'NODE_KEY_HERE'
   OR to_node_key = 'NODE_KEY_HERE'
ORDER BY weight DESC
LIMIT 50;"
```

Read graph clusters for a scene:

```bash
sqlite3 vector_databases/space_graph.sqlite "
SELECT cluster_key, cluster_type, region, label, summary,
       primary_node_key, confidence, member_count, retrieval_text
FROM graph_clusters
WHERE canonical_scene_id = 'SCENE_ID_HERE'
ORDER BY confidence DESC, member_count DESC
LIMIT 20;"
```

## SQL Server Checks

Use the read-only smoke query to confirm that the SQL Server WGD tables are
available:

```bash
export MSSQLTOP='your-password'
python3 wgd/connection/query_graph_vector_db.py --config wgd/connection/config.ini
```

Use JSON when another tool will consume the report:

```bash
python3 wgd/connection/query_graph_vector_db.py --config wgd/connection/config.ini --format json
```

The SQL Server tables normally use the `tmp_` prefix. The connection tool
prefers `tmp_` and falls back to unprefixed names.

## Sync After Construction

Preview what would be published:

```bash
python3 wgd/sync/wgd_sqlite_sync.py dry-run
```

Publish changed rows to `NODE010`:

```bash
python3 wgd/sync/wgd_sqlite_sync.py sync --verify-subscribers --verify-timeout-seconds 300
```

Sync only the tables touched by a construction pass:

```bash
python3 wgd/sync/wgd_sqlite_sync.py sync \
  --table scenes \
  --table povs \
  --table objects \
  --table materials \
  --table cameras \
  --table lights \
  --table spatial_nodes \
  --table spatial_edges \
  --verify-subscribers \
  --verify-timeout-seconds 300
```

Check recent sync state:

```bash
python3 wgd/sync/wgd_sqlite_sync.py status
```

Only purge missing rows when the source deletion is intentional:

```bash
python3 wgd/sync/wgd_sqlite_sync.py purge --confirm-purge
```

## Guardrails

- Keep the local SQLite databases as the construction source of truth until a
  scene is accepted.
- Prefer canonical-scene cohesion unless the brief explicitly asks for a remix.
- Preserve useful POV snippets, but rename declarations and normalize transforms
  so imported parts do not collide.
- Use graph edges and clusters to maintain layout, not just visual similarity.
- Treat material, light, and camera snippets as coupled choices when they come
  from the same source scene.
- Never use the full `mssqlldr/loader.sh` truncate/reload path during normal
  replication; it can create a large replicated delete/insert storm.
- Keep passwords out of config files. Use the configured password environment
  variable or `/tmp/mssqlpass`, depending on the tool.

## Construction Checklist

- Brief written with mood, subject, layout, and output target.
- Scene anchor selected from `scenes` plus `descriptions`.
- Object, material, camera, and light candidates inspected.
- Spatial nodes, edges, cells, or clusters checked for layout consistency.
- POV source composed or edited.
- Render inspected against the brief.
- Local vector indexes refreshed by the indexing pipeline.
- `dry-run`, `sync`, and `status` completed when publishing is needed.
