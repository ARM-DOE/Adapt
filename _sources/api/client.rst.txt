Repository Client API
=====================

Read-only interface for querying ADAPT pipeline output from a repository.
Initialise :class:`~adapt.api.RepositoryClient` with the repository root path;
it auto-discovers runs, radars, scans, and data items through the two-tier
database system (root-level registry + per-radar catalog).

.. code-block:: python

   from adapt.api import RepositoryClient, FilterSpec

   client = RepositoryClient("/data/radar_output")

   # List all runs
   for run in client.runs():
       print(run.run_id, run.status)

   # Fetch tracks for a run
   tracks_df = client.tracks(run_id)

   # Filter to severe cells only
   severe = client.select(run_id, FilterSpec(max_refl_min_dbz=55.0))

   # Load all products for one scan
   bundle = client.scan_bundle(scan_time, radar="KDIX")

   # Raw SQL escape hatch (DuckDB over Parquet)
   df = client.query("SELECT * FROM analysis2d WHERE refl_max > 40")


RepositoryClient
----------------

.. autoclass:: adapt.api.client.RepositoryClient
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
:meth:`~adapt.api.RepositoryClient.select`.

.. automodule:: adapt.api.selection
   :members:
   :undoc-members:
   :show-inheritance:
