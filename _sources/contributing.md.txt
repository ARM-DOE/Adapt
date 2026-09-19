# Contributing

```{note}
We are not accepting external Pull Requests at this time. Guidelines will be
published after our first stable release.

The workflow below is what the Adapt team follows, and what external
contributions will be expected to follow once they open.
```

## Setup, once

Fork the repo on GitHub, then clone your fork and point it at the project:

```bash
git clone https://github.com/<you>/Adapt.git && cd Adapt
git remote add upstream https://github.com/ARM-DOE/Adapt.git
```

`origin` is now your fork, `upstream` is `ARM-DOE/Adapt`.

Then install the pre-commit hooks. The repository ships the config; the hooks
run on every `git commit` and refuse the commit if a check fails.

```bash
pre-commit install
```

## Per change

1. **Open an issue and discuss it** before you start working.
2. **Branch off the current `main`.** Never commit to `main` itself.

   ```bash
   git fetch upstream
   git switch -c fix/short-name upstream/main
   ```

3. **One branch per logical change**, named for it: `fix/registry-fd-leak`,
   `feat/segmenter3D`. Not `wip`, not `my-changes`.
4. **Small, focused commits.**
5. **Run the checks locally before pushing.** Green CI is the contributor's job,
   not the reviewer's.
6. **Push to your fork and open the PR against `upstream/main`.** Link the issue
   (`Closes #123`), say what and why, and how you tested it. Open it as a draft
   early if you want direction before investing more.
7. **Respond to review with new commits** on the same branch. Don't force-push
   over commits already reviewed — it destroys the reviewer's place in the diff.

After it merges, delete your branch and start the next one from step 2 — the
`git fetch upstream` there is what keeps you current.

## Keeping current while the PR is open

- Rebase only while the branch is private and unreviewed.
- Once it's pushed and under review, merge `upstream/main` in instead.

```bash
git fetch upstream && git merge upstream/main
```

## The checks (step 5)

The pre-commit hooks cover ruff, ruff-format, mypy, import-linter and the
repository's own checks, and block the commit when one fails. Run the full set
against everything before you push:

```bash
pre-commit run --all-files
pytest
```

Fix failures — never suppress them. Don't commit with `--no-verify`: it skips
the hooks, and CI runs them anyway.

Integration tests are opt-in and excluded from a bare `pytest`: run them with
`pytest -m integration`.

Commit titles use the repository's prefixes — `ENH:`, `FIX:`, `TEST:`,
`REF:`, `DOCS:`, `CLEAN:` — followed by a short imperative summary.
Fill in `.github/pull_request_template.md` when you open the PR.
