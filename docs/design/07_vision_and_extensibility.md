# Vision and Extensibility

---

## What Adapt Is Building Toward

Adapt is designed to become a **live and historical geospatial sensor analysis platform** — a system where scientists and engineers can:

- Ingest data from radar, satellite, lightning networks, surface observations, and models
- Apply detection, tracking, and analysis algorithms that treat each data source as a first-class citizen
- Explore the resulting feature record interactively — live in a graphical environment or historically over any archive
- Compose processing pipelines programmatically for batch operations and autonomous monitoring
- Deploy in the cloud for operational use or run locally for experimentation

The current implementation handles NEXRAD radar data end-to-end. The architecture is designed so that every future sensor, algorithm, and consumer is an **addition**, never a **replacement** of existing code.

---

## Adding a New Data Source (e.g., Geostationary Satellite)

The pipeline does not know about NEXRAD specifically. It knows about:
- A `grid_ds` (an xarray Dataset on a Cartesian grid with canonical field names)
- A set of modules that transform data through the context dictionary

To add satellite data:

1. **Create `modules/acquisition_satellite/`** — a new thread-based downloader for satellite files (GOES, Himawari, etc.)

2. **Create `modules/ingest_satellite/`** — reads HDF5 or NetCDF satellite files, reprojects to a Cartesian grid, renames fields to canonical names, produces a `grid_ds` that satisfies `assert_gridded()`

3. **No changes needed** in detection, projection, analysis, or tracking — they operate on `grid_ds`, not on radar-specific data

4. Register both new modules in `defaults.yaml` under a satellite-specific configuration profile

The tracking module would then link cells detected in satellite data the same way it links radar cells. If you want to fuse radar and satellite (e.g., track cells seen in both sensors simultaneously), you add a fusion module that receives `grid_ds_radar` and `grid_ds_satellite` and produces a merged `grid_ds_fused`. No changes to the tracker.

---

## Adding a New Algorithm (e.g., Deep Learning Cell Detection)

The detection module is registered, not hardcoded. To replace the threshold-based segmenter with a U-Net:

1. Implement `BaseModule` in `modules/detection_unet/module.py`
2. Declare `inputs: ["grid_ds_2d", "config"]` and `outputs: ["segmented_ds"]`
3. Produce a `segmented_ds` that satisfies `assert_segmented()`
4. Register it in `defaults.yaml` instead of the current detection module

The projection, analysis, and tracking modules are completely unaware of which detection implementation ran. They see a `segmented_ds` in context that passed the contract. That is the only guarantee they need.

---

## Adding a New Consumer (e.g., Web API)

A web backend (FastAPI, Django, Flask) would:

1. Import `StoreClient` from `adapt.api`
2. Wrap `client.query()`, `client.latest()`, and `client.stream()` as HTTP endpoints
3. Serve JSON or GeoJSON to a frontend

The web layer has zero coupling to pipeline internals. It does not know about NEXRAD files, NetCDF format, or module implementations. It sees only the `StoreClient` interface.

For a streaming web dashboard, the server would hold a long-lived `client.stream()` generator and push updates to connected clients via WebSockets or Server-Sent Events.

---

## Operational Deployment

The current architecture runs as a single process with multiple threads. For cloud-scale deployment:

**Horizontal scaling**: Because each radar is independent (separate queues, catalogs, output directories), multiple instances can run in parallel — one per radar — without any shared state.

**Distributed pipeline**: The execution graph is a DAG of stateless operations. Each node could be a separate process (or container) connected by a message queue rather than in-memory dict passing. The context dictionary would become a message payload. The GraphExecutor would become a task dispatcher.

**Object storage**: The store currently writes to a local filesystem. Replacing the `ObjectStore`/catalog implementations with S3-backed ones (objects in object storage, a cloud database for the catalog) requires changing only the persistence layer. All other components are unaffected.

---

## The Invariants That Must Not Change

As the system evolves, three invariants must be preserved:

1. **The context dictionary protocol** — modules communicate only through context keys. No direct calls between modules.

2. **The contracts** — every stage boundary is validated. New modules must define and pass contracts. Contract violations must raise, not warn.

3. **The dependency direction** — `modules/` depends only on `contracts/`. Nothing imports from `modules/` except the execution layer. The consumer API depends only on `persistence/`.

These invariants are what make it safe to extend the system. Violate them and the system becomes a tangle of implicit dependencies that can only be maintained by a person who holds the entire design in their head.

---

## What Would Break the Architecture

| Action | Why It Breaks Architecture |
|--------|-----------------------------|
| A module imports from another module | Creates hidden coupling. One change in module A breaks module B silently. |
| Runtime layer contains science logic | Science becomes untestable without the full orchestration stack. |
| A module writes to disk directly | Persistence becomes scattered. Provenance tracking breaks. |
| Configuration is read at module runtime, not startup | Configuration is no longer frozen. Testing modules requires a full config stack. |
| A consumer writes to the repository | The read-only contract breaks. Pipeline and consumer contend on shared state. |
| Contracts are moved inside module `run()` | Contract violations become implementation errors. Boundary validation becomes invisible. |
