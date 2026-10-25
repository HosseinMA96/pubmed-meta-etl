# Changelog

All notable changes are listed here. Results must stay identical to `baseline/reference_stats.json` unless a change says otherwise.

## Unreleased

- Added `scale_test/`: Leiden-CPM runs on CENM with aligned, mean-normalized, and misaligned weights (script, configs, 47 per-run summaries, logs, environments; no per-edge data).
- Added `edge_order_check/`: shows that the thesis graph construction reorders edges, so weights passed by position do not reach their own edges; noted as a known issue in `thesis_pipeline/README.md`.
- Added `scale_equivalence/`: numerical check, on synthetic graphs, that scaling all weights by α is equivalent to resolution γ/α in CPM.
- Added `feature_audit/`: audit script of the CENM edge-feature table and a MeSH aggregation pilot, with aggregated results and CSV summaries (no per-edge data). Does not affect the parsing pipeline.
- Added `thesis_pipeline/`: cleaned copy of the thesis graph, feature, weighting and clustering scripts (credentials and server names replaced by environment variables; no logic changes) with a README.
- Added the MIT license.

- Credentials and lab-specific paths removed from `pubmed_etl.sh`, `pyspark_parquet.py` and comments; settings now come from `PUBMED_ETL_*` environment variables. The Parquet route is unaffected (verified: identical output on the 2-file sample).
- Added `baseline/` with reference numbers, checksums of the input files, and the script that recomputes them.
- Rewrote `README.md`; added `.gitignore`.
- History: a database password was redacted from two historical commits (hashes changed; authors, dates and messages unchanged).

## 0.0.0 (2023-12-15 to 2024-08-06)

Original research pipeline (see the commit history): XML extraction, parallel parsing to Parquet, Spark dedupe and flags, Postgres or Parquet output.
