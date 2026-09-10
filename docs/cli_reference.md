# CLI Reference

Adapt command structure: `adapt <command> [options]`

```
adapt init         # initialize an empty data store
adapt run-nexrad   # run the processing pipeline
adapt config       # generate a config.yaml template
adapt dashboard    # open the GUI dashboard
adapt postprocess  # enrich an existing repository with extension tables
```

`adapt --version` prints the installed version and the path Adapt was installed
to. `adapt --help` and `adapt <command> --help` print this same reference from
the command line.

---

## `adapt init`

Create the store layout: `registry.db`, `logs/`, `collections/`. This is the
only command that ever creates it — `adapt run-nexrad` and `adapt dashboard`
fail loudly on an uninitialized directory.

```bash
adapt init            # initialize the current directory
adapt init /data/kilx # initialize a specific directory
```

| Argument | Description |
|----------|-------------|
| `directory` | Root directory for the new store. Defaults to the current directory. Fails if the store already exists or the directory uses the obsolete pre-store layout. |

---

## `adapt run-nexrad`

Download and process NEXRAD Level-II data from AWS S3.

```bash
adapt run-nexrad [config.yaml] --radar KLOT --mode realtime
adapt run-nexrad [config.yaml] --radar KDIX --base-dir /data \
    --start-time 2025-03-05T15:00:00Z \
    --end-time   2025-03-05T18:00:00Z
```

### Positional argument

| Argument | Description |
|----------|-------------|
| `config` | Path to a config file (`.yaml` or `.py` with a `CONFIG` dict). Optional — expert defaults are used when omitted. |

### Options

| Flag | Default | Description |
|------|---------|-------------|
| `--radar SITE` | — | 4-letter NEXRAD site code, e.g. `KLOT`, `KDIX`, `KFTG` |
| `--mode` | `realtime` | `realtime` (continuous) or `historical` (fixed window) |
| `--start-time ISO` | — | Start of historical window, ISO 8601 (e.g. `2025-03-05T15:00:00Z`) |
| `--end-time ISO` | — | End of historical window, ISO 8601 |
| `--base-dir PATH` | — | Store root (must be initialized with `adapt init`) |
| `--run-id ID` | — | Resume a previous run by ID (format: `YYYYMONDD-HHMM-RADAR`); the run's configuration is reloaded from the store's registry |
| `--max-runtime MIN` | — | Stop after this many minutes (realtime mode only) |
| `--plot-dir PATH` | — | Directory for live PNG plots, outside the store. Plotting runs only when this is given |
| `--plot-interval SEC` | `2.0` | How often the plot consumer checks for new data (seconds) |
| `--show-plots` | off | Open a live window showing plots as they are produced |
| `--only NODES` | — | Comma-separated pipeline node names to run; skip the rest. Mutually exclusive with `--not` |
| `--not NODES` | — | Comma-separated pipeline node names to skip; run the rest. Mutually exclusive with `--only` |
| `-v`, `--verbose` | off | Enable DEBUG-level logging |

### Mode selection logic

If `--start-time` or `--end-time` is supplied, mode is automatically set to `historical`
even if `--mode` is not given explicitly.

---

## `adapt config`

Write a commented YAML configuration template to disk.

```bash
adapt config                     # writes ./config.yaml
adapt config /path/to/my.yaml   # writes to a specific path
```

| Argument | Description |
|----------|-------------|
| `output` | Destination path. Defaults to `./config.yaml`. If a directory is given, `config.yaml` is written inside it. |

| Flag | Default | Description |
|------|---------|-------------|
| `--pipeline NAME` | `nexrad` | Pipeline to generate a template for |
| `--extensions PATHS` | — | Comma-separated extension module import paths to include (e.g. `adapt.execution.nodes.cell_volume_stats`) |

The generated file includes all tunable parameters with inline comments.
Edit it, then pass it as the first argument to `adapt run-nexrad`.

---

## `adapt postprocess`

Enrich an existing repository with post-processing extension tables (e.g.
associating lightning or other external observations with completed storm
tracks).

```bash
adapt postprocess --module xlma_stat
adapt postprocess --repository /data/radar --module xlma_stat --input-dir /data/lma -v
```

| Flag | Default | Description |
|------|---------|-------------|
| `--repository PATH` | `.` | Repository to enrich |
| `--module MODULE [MODULE ...]` | — | Post-processing module(s) to run |
| `--input-dir PATH` | — | Directory containing the external dataset to associate |
| `--config PATH` | — | Config file with module-specific settings |
| `-v`, `--verbose` | off | Enable DEBUG-level logging |

---

## `adapt dashboard`

Launch the read-only GUI dashboard.

```bash
adapt dashboard
adapt dashboard --repo ~/adapt_output
```

| Flag | Description |
|------|-------------|
| `--repo PATH` | Pre-populate the repository path field in the GUI. |

The dashboard reads from an existing output repository. It does not start or
affect a running pipeline. Run it in a second terminal while the pipeline runs.
