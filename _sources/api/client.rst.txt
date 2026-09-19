Store Client API
================

Read-only interface for querying an Adapt data store (a directory created by
``adapt init``). Initialise :class:`~adapt.api.StoreClient` with the store root
path; it discovers collections, runs, scans, tracks, and every module's product
table through the store's catalogs — no file paths, no directory walking.

.. code-block:: python

   from adapt.api import StoreClient

   client = StoreClient("/data/radar_output")

   # Discover collections and runs
   for run in client.runs("KLOT"):          # newest first
       print(run.run_id, run.status)
   run = client.latest_run("KLOT")

   # Complete scans only, identity-keyed (scan_id = sha256(raw bytes)[:16])
   timeline = client.scan_timeline("KLOT", run.run_id)
   with client.open_scan_raster("KLOT", run.run_id, timeline[-1].scan_id) as raster:
       raster.dataset  # fully in-memory xarray.Dataset

   # Tracking
   cells = client.cells(run.run_id, "KLOT")
   history = client.track_history(run.run_id, "SOME_UID", "KLOT")
   graph = client.track_graph(run.run_id, "SOME_UID", "KLOT")  # split/merge component

   # Any module's product table, with typed operator filters
   severe = client.table("cell_tracks", "KLOT", run_id=run.run_id,
                         filters={"max_reflectivity": {"op": "gt", "value": 50.0}})

   # Arbitrary read-only SQL (catalog attached as ``catalog``)
   df = client.sql("SELECT COUNT(DISTINCT cell_uid) AS n FROM cells_by_scan", "KLOT")

   client.close()

Cross-run queries (``run_id=None``) union across runs and every row carries its
``run_id``. Scan listings return complete scans only, so a consumer never
observes a half-written scan. ``scans_since`` provides a typed watermark for
live-follow polling.

StoreClient
-----------

.. autoclass:: adapt.api.store_client.StoreClient
   :members:
   :undoc-members:
   :show-inheritance:


Domain Objects
--------------

Immutable dataclasses returned by the client methods.

.. automodule:: adapt.api.domain
   :members:
   :undoc-members:
   :show-inheritance:


FilterSpec
----------

Immutable filter compiled to a SQL ``WHERE`` clause by
:meth:`~adapt.api.StoreClient.select`.

.. automodule:: adapt.api.selection
   :members:
   :undoc-members:
   :show-inheritance:
