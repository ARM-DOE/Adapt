Processing Modules
==================

Each module has a single, well-defined responsibility in the pipeline.
Modules communicate exclusively through a shared context dictionary — never by importing each other.

.. toctree::
   :maxdepth: 1

   acquisition
   ingest
   detection
   projection
   analysis
   tracking
