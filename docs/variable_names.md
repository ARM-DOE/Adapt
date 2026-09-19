# Variable Names and the Tracking Field

Adapt works with any radar source and, in principle, any 2-D scalar field.
Two independent bindings make that possible, and each lives in exactly one
place:

| Binding | Config knob | Applied where |
|---|---|---|
| Source name → canonical name | `reader.field_map`, `reader.fields` | Once, inside the ingest module |
| Role → canonical field | `global.tracking_field` | Read by detection / projection / tracking |

Everything downstream of ingest — module code, contracts, stored column
names, the dashboard, the Target Selection Engine — speaks **canonical
names only** and never maps names again.

## Canonical names

The canonical vocabulary is simply Adapt's established names:
`reflectivity`, `velocity`, `differential_reflectivity`,
`spectrum_width`, `cross_correlation_ratio`, `differential_phase`, …
The segmentation label grid is always `cell_labels` (pipeline-internal,
not configurable). Per-cell statistics columns are always minted as
`radar_<field>_<stat>` (e.g. `radar_reflectivity_max`) — the `radar_`
prefix is a namespace, not a physics claim.

## Renaming source variables (`reader.field_map`)

If your input files name a field differently — ARM CMAC calls reflectivity
`corrected_reflectivity` — declare the rename once:

```yaml
reader:
  field_map:
    corrected_reflectivity: reflectivity
    corrected_differential_reflectivity: differential_reflectivity
  fields: [reflectivity, differential_reflectivity]
```

- `field_map` maps **source name → canonical name** and is applied inside
  ingest, immediately after regridding. Map entries whose source variable
  is absent from a given file are ignored.
- `fields` is the explicit keep-list (canonical names, checked after the
  renames). An empty list keeps every field the file provides. If a
  listed field is missing, the run fails loudly naming what was available.
- The legacy `REFLECTIVITY_VAR: dbz` user setting still works and means
  exactly this: `field_map: {dbz: reflectivity}`.

The applied mapping is recorded as provenance: in each grid's
`source_fields_json` attribute and in the run's stored configuration
(`registry.db → runs.config_json`), readable via
`StoreClient.run_config(run_id)`.

## Choosing the tracked field (`global.tracking_field`)

Detection thresholds on, projection flows on, and tracking links cells on
one canonical field — `reflectivity` by default:

```yaml
global:
  tracking_field: reflectivity   # or any canonical field, e.g. pressure
analyzer:
  radar_variables: [reflectivity, velocity]   # must include tracking_field
```

Two rules are enforced at configuration time (the run refuses to start
otherwise):

1. `tracking_field` must be in `analyzer.radar_variables` — otherwise the
   per-cell statistics the tracker reads are never computed.
2. If `reader.fields` is non-empty, it must include `tracking_field`.

Dual-polarization fields are **optional**: a source without
`differential_reflectivity` runs the full pipeline; ZDR-derived columns
simply do not exist for that run (consumers discover available columns
through the store's `table_schemas`).

## Cell identity (uid v2)

Cell uids are `hash(scan_id, cell_label)` — a pure function of the scan's
raw bytes and the cell's label. Identical input and configuration always
mint identical uids, for any source and any tracked field. Runs recorded
before v2 keep their stored uids (uids are minted once and never
recomputed; every table scopes them by `run_id`). Cross-run comparisons
join on `scan_id` + `cell_label`.

## How consumers resolve the field

Consumers never guess. The run's provenance is the truth:

```python
client.run_config(run_id)          # full resolved config (dict)
client.run_tracking_field(run_id)  # the canonical tracked field
```

`run_tracking_field` also understands pre-v2 provenance (runs that
recorded `var_names.reflectivity`), so old stores stay readable. The
dashboard backdrop, hover statistics, and the Target Selection Engine's
`CellSnapshot.field_max` all resolve through it.

## Compatibility notes

- **Existing stores** remain readable unchanged; canonical names equal the
  names Adapt always used.
- **Changed field sets need a new collection**: a collection's product
  tables freeze their column set on first write, so a run whose
  `fields` / `field_map` / analyzer whitelist produces different columns
  is rejected by an existing collection (by design — create a new one).
- **Saved TSE YAML**: the priority weight key `reflectivity:` is now
  `field:` (it weights the tracked field's max). Old files fail loudly at
  load with a message naming the unknown key.
- **`cell_tracks.max_reflectivity`** (and `Track.max_reflectivity_dbz`,
  `FilterSpec.max_refl_*`): the value is the maximum of the run's
  *tracked field*; the name is kept for store compatibility and only
  means dBZ for reflectivity-tracked runs.

## Onboarding a new source, in practice

1. Write (or reuse) an ingest path that reads the format.
2. Add the source's `reader.field_map` and an explicit `reader.fields`.
3. Pick `global.tracking_field` and match `analyzer.radar_variables`.
4. Run into a **new collection**.

No detection, projection, tracking, persistence, api, or consumer code
changes are required.
