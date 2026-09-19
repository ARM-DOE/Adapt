Runtime
=======

Composes the pipeline over the data store: the acquisition gateway mints scan
identity at the source boundary, the processor drives the module graph per
scan, the orchestrator manages run lifecycle in the store registry, and the
post-processor runs after-the-fact modules into an existing store.

.. automodule:: adapt.runtime.orchestrator
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: adapt.runtime.processor
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: adapt.runtime.acquire
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: adapt.runtime.postprocessor
   :members:
   :undoc-members:
   :show-inheritance:
