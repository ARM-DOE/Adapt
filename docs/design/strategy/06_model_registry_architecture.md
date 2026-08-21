# Document 6 — Model Registry Architecture

Scope: a registry for machine-learning models, traditional algorithms, scientific
retrievals, and calibration products. Nothing exists today — this is greenfield design,
which is the right time to apply the constitution before habits form.

## Framing: a model is two things Adapt already has

Strip the MLOps vocabulary and a "model" is:

1. **A versioned artifact with provenance** — bytes on disk, plus metadata about what
   produced them. Adapt has an artifact catalog (Doc 04).
2. **A module at inference time** — `inputs → compute → outputs`. Adapt has modules
   (Principle 2).

So the design is deliberately unexciting: **extend the artifact catalog with a model
registry table, and run inference through ordinary modules that pin a model by
name+version in their config.** No new execution concepts, no model server, no new
service. The same mechanism covers traditional algorithms and calibration products,
because they differ from ML models only in file format and size.

## Registry data model

A registry database (SQLite, same engine and patterns as the run catalog — but
**separate from per-run catalogs**, because models outlive runs and are shared across
them):

```
models(
  name, version,                 -- PK; SemVer set by the publisher
  kind,                          -- ml_model | calibration | lookup_table | algorithm_params
  format,                        -- onnx | pytorch | netcdf | json | ...
  file_path, sha256, size_bytes,
  stage,                         -- candidate | approved | operational | retired
  created_at, created_by,
  training_run_id,               -- lineage into an Adapt run manifest, when trained in-platform
  card_json                      -- free-form model card: task, domain, training data
                                 -- description, known limitations, citation
)
model_metrics(name, version, dataset_id, metric, value, computed_at, run_id)
model_events(name, version, event,     -- registered | promoted:<stage> | retired
             actor, at, note)          -- the audit trail; promotions are events, not UPDATEs in place
```

Storage layout mirrors the existing repository conventions:
`<base_dir>/models/{name}/{version}/model.onnx` + `card.json`. Files are immutable
once registered (same atomic-write + hash discipline as the store's `ObjectStore`).

### Versioning and reproducibility rules

- A `(name, version)` pair is **immutable** — re-registering with different bytes is an
  error, enforced by hash comparison. "Latest" is a query convenience, **never** valid
  in pipeline config: a pipeline that says `version: latest` fails config validation,
  because it would make the run manifest non-reproducible (Principle 5 trumps
  convenience).
- The run manifest (Doc 04) records every model used: name, version, sha256. An
  artifact produced by a model-backed module is traceable to the exact bytes.
- When training happens inside Adapt (a training pipeline is just another pipeline
  document), `training_run_id` closes the loop: artifact → model → training run →
  training data. When models arrive from outside, the model card carries the
  description and the field is null — honest, not fabricated.

## Inference: models are modules

```python
class HailClassifier(BaseModule):
    name = "hail_classifier"
    inputs = ["cell_stats", "grid_ds_3d"]
    outputs = ["hail_scores"]
    config_class = HailClassifierConfig   # model_name: str, model_version: str, ...

    def run(self, context):
        model = context["model_store"].load(self.cfg.model_name, self.cfg.model_version)
        ...
```

- The framework injects a read-only `model_store` handle the same way it injects
  repository-backed inputs today (the `grid_ds_3d` on-demand pattern in
  `processor._build_enrich_context`). Modules never touch model file paths.
- **Format adapters live in the adapters layer** (the one place third-party calls are
  allowed): `onnxruntime` first — it is the recommended interchange because it
  decouples training stacks from the operational environment — with torch/sklearn
  adapters as optional extras in `adapt-ml`. The kernel knows `bytes + format`, never
  imports a framework.
- Loaded models cache in-process by `(name, version)` — they are immutable, so this is
  safe and keeps per-scan latency flat.

This means **the execution engine needs zero changes for ML.** A model-backed detector
competes with the threshold detector by registering under a different module name and
being selected in the pipeline document — exactly the algorithm-swap story the
registry was built for.

## Promotion workflow

Stages: `candidate → approved → operational → retired`.

- Promotion is a **CLI operation writing an audit event**, not a config edit:
  `adapt model promote hail_classifier 1.3.0 --to approved --note "eval vs 2025 season"`.
- Policy enforcement is configuration, not code: an operational deployment sets
  `model_policy: operational` and the model store refuses to load anything below that
  stage; research configs allow `candidate`. The check is at load time, fail-loud.
- Demotion/retirement never deletes bytes — retired models must stay loadable forever,
  because published results reference them (reproducibility outlives operations).
- Approval criteria (which metrics, which evaluation datasets, who signs off) are
  governance (Doc 10), not schema — the schema only guarantees the decision is
  *recorded*.

## Model comparison

Comparison is a query, not a feature:

- Evaluation datasets are registered artifacts with IDs (a labelled scan set is itself
  a product with provenance).
- An evaluation pipeline (ordinary pipeline document) runs N model versions over a
  dataset and writes `model_metrics` rows keyed `(name, version, dataset_id, metric)`.
- `adapt model compare hail_classifier --versions 1.2.0,1.3.0 --dataset eval_2025q2`
  prints the table. Plotting belongs in Ring 2 consumers.

Because metrics rows carry `run_id`, every number in a comparison is itself traceable
to the run that computed it — provenance applies to evaluations, not just science.

## Buy vs build — evaluated

| Option | Assessment |
|--------|------------|
| **MLflow** | The default industry answer, and wrong for Adapt's center of gravity: a tracking *server* + UI + its own artifact store, duplicating Adapt's catalog and provenance with a second source of truth. Operational complexity criterion fails for edge/site deployments (radar sites won't run an MLflow server). **Rejected as core. Accepted as exporter**: a thin `adapt-mlflow` consumer can mirror the registry for teams already living in MLflow. |
| **W&B / Neptune** | SaaS; data-residency and air-gapped site constraints rule them out as a dependency. Same exporter posture. |
| **DVC** | Git-anchored data versioning; wrong shape for an operational store queried by running pipelines. Rejected. |
| **HuggingFace Hub** | Distribution channel, not a registry; potentially useful later for *publishing* approved models. Out of scope. |
| **Build (above)** | ~2 tables + a store class + 4 CLI verbs on infrastructure that already exists. The registry's availability story equals the pipeline's (a file tree + SQLite), which is exactly right for sites. |

The deciding argument: Adapt already owns artifact identity, hashing, lineage, and
run manifests. Importing a second provenance system to avoid two SQLite tables would
violate Principles 1 and 10 simultaneously.

## What not to build (YAGNI fence)

- No model serving endpoints — inference is in-pipeline; there is no request/response
  consumer.
- No automatic retraining triggers, drift detection, or champion/challenger
  automation — these are pipelines and governance, composable later from existing
  parts; building them now is speculation.
- No experiment-tracking UI — `model_metrics` + notebooks via the read API.
- No per-tensor signature validation — the module's own input/output DataSpecs
  (Doc 05) already validate what enters and leaves; duplicating ONNX-level shape
  checks adds a second contract layer.

## Migration order

1. `ModelStore` + `models`/`model_events` tables + `register`/`list`/`promote`/`show`
   CLI. (Useful immediately for calibration files, before any ML lands.)
2. `model_store` context injection + ONNX adapter in `adapt-ml`.
3. Manifest integration (models used per run).
4. `model_metrics` + evaluation pipeline conventions + `compare`.
5. Optional MLflow exporter, on demand.

## Scoring

| Criterion | Score | Rationale |
|-----------|-------|-----------|
| Contributor friendliness | High | An ML contributor writes one ordinary module; registry is 4 CLI verbs |
| Extensibility | High | New formats = new adapter; new kinds (calibration, LUTs) = enum value |
| Testability | High | Store is files + SQLite; fake with tmpdir; no network, no mocks of services |
| Reproducibility | High | Immutable versions, hash-verified, pinned in manifests; audit-trailed promotion |
| Operational complexity | Low | Zero new services; works air-gapped at a radar site |
| Long-term maintainability | High | Two tables and a loader; external tools attach as exporters, not foundations |
