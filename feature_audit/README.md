# Feature-table audit

Scripts and summary results behind Section 6 ("Audit of the Feature Table") of the paper
*Building Weighted PubMed Citation Graphs at Scale: A Pipeline and an Audit of Edge-Similarity Features*
(H. Mohasel Arjomandi, 2026). This audit is new work; it was not part of the M.S. thesis.

Only aggregated statistics are published here. The per-edge feature table (CENM, 57,512,086 edges) and the pilot metadata are not released, because they derive from the research group's network data.

| File | Content |
|---|---|
| `feature_audit.py`, `config.json` | Audit of the per-edge feature table: counts, ranges, distributions, Spearman correlations (2,000,000-edge sample, seed in `config.json`), agreement between topological and semantic features, near-identical texts, the [1,10] weight map. Set `parquet` to your copy of the table. |
| `results.json` | Full output of `feature_audit.py`. |
| `feature_distributions.csv` | Summary behind Table IV. |
| `agreement_by_bib_jaccard.csv` | Summary behind Table V. |
| `spearman_sample.csv` | Spearman matrix (features f1-f7). |
| `mesh_pilot.py`, `mesh_pilot_config.json`, `mesh_pilot_results.json` | MeSH aggregation pilot (median vs mean vs best match) on all pairs of 1,591 articles of one cluster. |

Run (Python 3.10, numpy, pyarrow, scipy):

```sh
python3 -I feature_audit.py config.json results.json
python3 -I mesh_pilot.py mesh_pilot_config.json mesh_pilot_results.json
```

`feature_audit.py` reads one column at a time and peaks at about 4-5 GB of RAM.
