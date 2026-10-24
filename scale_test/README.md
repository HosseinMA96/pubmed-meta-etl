# Weight-scale clustering runs (Leiden-CPM on CENM)

Script, configurations, logs and per-run summaries behind the new experiments (Section 6) of the paper
*Similarity-Weighted Leiden-CPM Clustering of a 7.4-Million-Node Biomedical Citation Graph*
(H. Mohasel Arjomandi, 2026, in preparation). These runs are new work; they were not part of the M.S. thesis.

Only aggregated statistics are published. The per-edge feature table (CENM, 57,512,086 edges) is not released,
because it derives from the research group's network data. Set `parquet` in `config.json` to your copy.

| File | Content |
|---|---|
| `run_cpm.py` | Builds a thesis weight setting, optionally transforms it (`raw`, `mean_norm`, `mean_norm_floor`, `mean_norm_zeromean`, `map110`) or misaligns it (`nx_order`, `shuffle`), runs igraph Leiden-CPM, and writes coverage, CPM score and intra-cluster coherence. Current version v5 (adds per-run `n_iterations`, igraph's reported quality, and the cluster-size histogram). |
| `run_cpm_v1.py`–`run_cpm_v4.py`, `config_v1.json`, `run_all_v1.sh` | Frozen earlier versions; `runs_manifest.csv` says which version produced each run. |
| `config.json` | Global settings (γ, engine, weight settings, shuffle seed) and one entry per run. |
| `run_all.sh`, `run_list.sh` | Sequential drivers; each run is wrapped in `systemd-run` with a memory cap. |
| `runs/*.json` | One summary per run. |
| `RESULTS.md`, `runs_manifest.csv` | Results table, seed spread, and per-run provenance, written by `summarize.py`. |
| `batch_log.txt` | Timeline of every run attempt, including the two runs killed at an 11 GB cap and rerun at 12 GB. |
| `determinism_check.py`, `determinism_check_log.txt` | Same seed gives an identical partition. |
| `igraph_objective_check.py`, `igraph_objective_check_log.txt` | igraph's CPM quality equals Σ_c (e_c − γ C(n_c,2)) − γN/2 on random graphs, so it has the same optimal partitions as `leidenalg`'s CPM. |
| `feasibility_*.txt` | First runs; `leidenalg` exceeded a 10 GB cap on this graph, so the runs use igraph's Leiden. |
| `envs/` | Package versions (`leiden_conda.yml`, `leiden_pip_freeze.txt`), machine, input checksums. |

Run (conda env from `envs/leiden_conda.yml`):

```sh
PY=python   # the leiden env
$PY -I run_cpm.py config.json prep        # remapped edge list -> work/ (about 1.3 GB)
$PY -I run_cpm.py config.json prep_nx     # thesis (networkx) edge order -> work/nx_order.npy
./run_all.sh > batch_log.txt 2>&1         # every configured run without a JSON
python3 -I summarize.py
```

A run takes 11–34 minutes and 6.5–11.9 GB on an 8-thread laptop. Local paths in the configs and logs were replaced by
`/path/to/` or `~`, so `code_checksums.sha256` matches the scripts but not those path lines.
The edge-order check of Section 6.1 is in [`../edge_order_check/`](../edge_order_check/).
