# Document 10 — Open-Source Sustainability Plan

Premise: Adapt becomes a widely used scientific platform — dozens of contributors,
multiple institutions, a plugin ecosystem, decade-plus lifetime. Sustainability is a
design problem with the same shape as the code architecture: a small stable core
(governance, contracts, releases) and everything else pushed to the edges
(extensions, distributions, downstream packages).

## Where the project actually is

BSD-3 licensed under the ARM-DOE org; CI matrix (Linux/macOS × 3.12/3.13) with ruff,
import-linter, license-header, and coverage gates; docs site + PyPI release
automation; conventional ARM-style commit prefixes (`ENH:`/`FIX:`/`DOC:`) in use; a
README that explicitly says *"not accepting external PRs; expect breaking changes."*

That closed-alpha stance is **correct today** — pretending to be open while the
contracts churn would burn early contributors. The plan below is about making the
opening deliberate instead of accidental.

## Governance — phased, not aspirational

| Phase | Trigger | Structure |
|-------|---------|-----------|
| 1 (now) | — | Single-maintainer / sponsor-driven. Formalize only two things: **ADRs** (architecture decision records — `docs/design/strategy` and `docs/design/issues` are already proto-ADRs; give them numbers and a status field) and a public **roadmap** so outsiders can see where help will be wanted |
| 2 | First sustained external contributor, or second in-tree domain | Maintainer team (2–4), CODEOWNERS per layer (contracts and persistence get the strictest ownership), lazy-consensus on PRs, maintainer veto on Ring 0 |
| 3 | Second *institution* with maintainers | Lightweight steering group (one rep per institution + lead); decides contracts/releases/deprecations only — science direction stays with domain distributions. Adopt a proven template (NumFOCUS-style) rather than drafting from scratch |

Two failure modes to design against explicitly: **single-funder fragility** (DOE/ARM
priorities shift — the mitigation is multi-institution maintainership in Phase 3 and
keeping the kernel useful beyond ARM radars) and **drive-by-PR overload** (the
mitigation is the contribution ladder below, which gives 90% of contributions a home
that needs no core review at all).

## Contribution workflow — the ladder is the policy

1. **Rung 1 — your own repo.** The extension template (Doc 02) + entry points means
   anyone ships a module without asking permission. The project maintains a
   *curated list page* of known extensions (a markdown file, not infrastructure).
   Most contributions should live here forever; this is the load-bearing scalability
   mechanism.
2. **Rung 2 — `adapt-contrib`.** Lower review bar, faster releases, explicit
   "best-effort stability" banner. One maintainer approval.
3. **Rung 3 — core distributions (`adapt-core`, `adapt-radar`).** Two approvals;
   contract changes additionally require an RFC.

**RFC process** (issues with a template, not a new tool) is mandatory only for:
contract/spec changes, new Ring 0 concepts, deprecations. Everything else is just a
PR. Keep the bureaucracy proportional to blast radius.

**Review workflow:** CI is already the first reviewer (ruff, import-linter,
architecture tests, coverage) — extend it with the spec-validation and determinism
tests (Docs 04/05) so human review is about *science and design*, not mechanics.
Document review SLAs honestly (e.g., first response in 2 weeks) — unanswered PRs kill
communities faster than rejected ones.

## Release strategy

- **SemVer, staying 0.x until contracts v1** (Doc 05) is frozen — 1.0.0 *means* "the
  module interface, DataSpecs, and read API are stable." Don't let 1.0 happen by
  drift; make it the explicit contracts-v1 milestone. (CalVer was considered —
  appropriate for data products, wrong for a platform whose consumers need
  compatibility semantics in the version number.)
- **Release train:** time-based minors (every ~2 months) rather than feature-based —
  predictability beats heroics; `setuptools_scm` + the existing PyPI workflow already
  support it. Patch releases on demand for operational fixes.
- **One LTS line per year once sites deploy operationally** (sites can't chase the
  train), receiving fixes for ~18 months. This is the cost of being operational
  infrastructure; budget for it.
- **Data products are versioned independently:** a `schema_version` table in
  catalog/track-store databases and `Conventions`/schema attrs in NetCDF (Doc 04/05),
  with migration notes per release. A user must be able to tell, from a file alone,
  which Adapt schema wrote it.

## API stability and deprecation policy

Define the **public surface** explicitly (a docs page, enforced by convention and
`__all__`):

- Public: `adapt.sdk` (module-author surface), `adapt.contracts` (specs + versions),
  `adapt.api` (read client), the CLI grammar, config file schema, on-disk schemas.
- Internal everything else (`runtime`, `execution`, `persistence` internals) — no
  guarantees, signposted in docs.

Deprecation policy, written down and mechanical:
1. Deprecate in release N: `DeprecationWarning` naming the replacement + changelog
   entry + docs banner. The "no fallbacks" constitution applies to *behavior*, not to
   API transitions — a warned alias for two minors is the courtesy a platform owes
   its users.
2. Remove in N+2 minors (or next major). Never silently change semantics — remove
   loudly instead.
3. CLI flags and config keys follow the same N+2 rule (config resolver warns on
   deprecated keys — it already errors on unknown ones, which is the right strictness).
4. On-disk schema changes ship with a migration script or an explicit
   "reprocess required" statement. No silent readers-guess-the-version.

## Documentation strategy

Adopt **Diátaxis** explicitly — the current docs already cluster that way; name the
quadrants and assign owners:

| Quadrant | Artifact | Source of truth |
|----------|----------|-----------------|
| Tutorial | 15-minute first-module (Doc 07) | Hand-written, CI-executed so it can't rot |
| How-to | Deploy a site, run a reprocessing campaign, publish an extension, promote a model | Hand-written |
| Reference | Module catalog, **spec catalog (auto-generated from DataSpecs)**, CLI, config schema (auto-generated from Pydantic), API | Generated wherever possible — generated reference is the only kind that stays true |
| Explanation | `docs/design/` (current-state) + this strategy set + ADRs | Maintainer-owned |

Versioned docs (per minor) once 1.0 lands. The sample-data bundle (a few bundled
scans) is a documentation asset as much as a testing one — every tutorial and CI doc
run uses it.

## Community health checklist

- `CONTRIBUTING.md` at repo root stating the ladder, review SLAs, and the constitution
  (the ten principles are a *better* contributor orientation than most style guides).
- Code of conduct at repo root (one exists only inside `.claude/skills/` vendored
  content today — adopt Contributor Covenant at top level).
- Issue/PR templates that route: bug vs science question vs extension announcement vs
  RFC.
- Recognition: extensions list + release-notes credits + CITATION.cff (+ a JOSS/AMS
  software paper when stable — `scipy_paper.tex` in the repo suggests this is already
  in motion; citability is how scientific software earns maintenance funding).
- Triage rotation once Phase 2 exists; "good first module" labels pointing at wanted
  extensions rather than core internals — first contributions should land on rung 1
  or 2, where success is likely.

## Scoring

| Criterion | Score | Rationale |
|-----------|-------|-----------|
| Contributor friendliness | High | Ladder gives a no-permission path; SLAs and templates set expectations |
| Extensibility | High | Ecosystem grows in extension repos, not in the core review queue |
| Testability | High | CI-as-first-reviewer extended with determinism/spec gates; executable tutorials |
| Reproducibility | High | LTS + lock files + schema versioning keep published science re-runnable for years |
| Operational complexity | Low | Governance artifacts are markdown files and labels, not services |
| Long-term maintainability | High | Stability promises scoped to a tiny public surface; everything else may evolve |
