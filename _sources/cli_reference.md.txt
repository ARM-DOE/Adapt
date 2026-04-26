# CLI Reference

Adapt command structure: `adapt <command> [options]`

```
adapt run-nexrad   # run the processing pipeline
adapt config       # generate a config.yaml template
adapt dashboard    # open the GUI dashboard
```

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
| `--base-dir PATH` | — | Root output directory for all artifacts |
| `--run-id ID` | — | Resume a previous run by ID (format: `YYYYMONDD-HHMM-RADAR`) |
| `--max-runtime MIN` | — | Stop after this many minutes (realtime mode only) |
| `--rerun` | off | Delete existing output for this radar before starting |
| `--no-plot` | off | Disable the plot consumer thread |
| `--plot-interval SEC` | `2.0` | How often the plot consumer checks for new data (seconds) |
| `--show-plots` | off | Open a live window showing plots as they are produced |
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

The generated file includes all tunable parameters with inline comments.
Edit it, then pass it as the first argument to `adapt run-nexrad`.

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
