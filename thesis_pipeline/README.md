# thesis_pipeline: citation graph features and clustering

Research code (from the M.S. thesis below) that turns a citation graph plus PubMed metadata into a graph whose edges carry seven similarity features, combines the features into user-weighted edge weights, and clusters the weighted graph with Leiden-CPM.

It is a cleaned copy of the thesis pipeline in [illinois-or-research-analytics/hm31_graphs](https://github.com/illinois-or-research-analytics/hm31_graphs), restricted to the scripts the thesis and the two workshop papers describe. Credentials and the lab server name were removed; nothing else in the logic was changed.

> **Status:** research code, not packaged, not tested in this form. Paths, table names and settings come from the original runs and must be adapted. Licensed under the MIT license of this repository (see `../LICENSE`).

## Background
- Thesis: H. Mohasel Arjomandi, *A Framework for Similarity-Based Clustering of Biomedical Citation Graphs*, M.S. thesis, University of Illinois Urbana-Champaign, 2024.
- Metadata table: [Parsed Open Citations and PubMed Data](https://doi.org/10.13012/B2IDB-5216575_V1) (Illinois Data Bank, CC BY 4.0), produced with [pubmed_etl](https://github.com/illinois-or-research-analytics/pubmed_etl).
- Embeddings: MedCPT PubMed article embeddings, <https://ftp.ncbi.nlm.nih.gov/pub/lu/MedCPT/pubmed_embeddings/>.

## What you need
- A citation edge list `(first, second)` (first cites second) and a node list mapping node IDs to DOIs, in PostgreSQL.
- The PubMed metadata table from `pubmed_etl` (PMID, lower-cased DOI, year, MeSH, title/abstract flags), in PostgreSQL.
- Python 3, PySpark, pandas, NumPy, psycopg2, NetworkX, python-igraph, leidenalg, pandarallel; the PostgreSQL JDBC driver for Spark.

## Configuration
Set these environment variables instead of editing the scripts:

| Variable | Used by | Meaning |
|---|---|---|
| `CGF_JDBC_URL` | Spark scripts | e.g. `jdbc:postgresql://HOST:5432/DBNAME` |
| `CGF_DB_USER`, `CGF_DB_PASSWORD` | Spark scripts | database login |
| `PGHOST`, `PGUSER`, `PGPASSWORD`, `PGDATABASE` | psycopg2 scripts (`features/calculate_*_feature.py`) | standard libpq variables |

The scripts also contain absolute scratch paths (`/shared/...`) and table names in the schema `hm31`; change them for your setup.

## Pipeline (stage numbers as in the BTSD paper, Table I)
| Stage | What | Where |
|---|---|---|
| 1 | Parse PubMed XML, keep one record per DOI, latest revision per PMID | the parser at the root of this repository |
| 2-4 | Join nodes to PubMed by DOI, keep records with year, MeSH, title and abstract, keep the highest-degree node per PMID, keep edges with both endpoints | `sql/steps.sql` (Steps 1-4) |
| 5 | Year and MeSH similarity per edge (Spark) | `features/calculate_similarities.py` |
| 6 | Bibliographic coupling (count, Jaccard) from out-neighbor lists | `sql/steps.sql` (Steps 6-7) + `features/calculate_similarities.py` |
| 7 | Co-citation (count, Jaccard) from in-neighbor lists, chunked, 40 processes | `sql/steps.sql` + `features/calculate_cocitation_feature.py` |
| 8 | MedCPT embeddings: download, join by PMID, cosine per edge | `embeddings/*.py`, `sql/steps.sql` (Step 9), `features/calculate_embedding_feature.py` |
| 9 | Merge features, min-max scale `f1-f4, f7`, combine with user weights | `sql/steps.sql` (Step 11), `weighting/normalization_and_dumping.py`, `weighting/weighting.py`, `clustering/transformation.py` |
| - | Leiden-CPM clustering, coverage and CPM statistics, weight sweeps | `clustering/construct_base_graph.py`, `clustering/weighting_clustering_pipeline.py` |

`sql/steps.sql` is the log of the queries as they were run, with row counts in comments.

## Behaviour worth knowing
- **Missing neighbor lists:** an edge has no coupling or co-citation row only when an endpoint has no references or no citing papers in the graph. The merged table sets these values to 0, which is the exact value.
- **Missing embeddings:** if either endpoint has no embedding (or one with at most 100 entries), the cosine feature is set to 0.5.
- **Cosine:** computed as the raw dot product over the norms, with no clipping.
- **MeSH:** `features/mesh.json` maps each of 29,944 descriptors to one tree number. A term pair's similarity is the number of shared leading components (the letter counts as one); the edge value is the median over all pairs; terms missing from the lookup are skipped; 0 if no pair remains.
- **Scaling:** Spark `MinMaxScaler` on `f1-f4` and `f7`; year and cosine are used as computed.
- **Rescaling to [1,10]** (`apply_weight_scaling`): zero weights are set to the smallest nonzero weight, then all weights are mapped linearly to [1,10], separately for each weight setting.
- **Reported CPM score:** computed on the unweighted graph (number of internal edges) at the given resolution, not on the weighted objective that Leiden optimizes.
- **Sweeps:** the topological sweep script runs strengths 0 to 0.875 in 8 steps; Leiden uses seed 4311 by default.
- **Known issue, weight–edge alignment:** `clustering/weighting_clustering_pipeline.py:leiden` passes the weight column to `leidenalg` by position, but `construct_base_graph.py` builds the graph through networkx, which reorders the edges. The weights therefore do not reach their own edges. See [`../edge_order_check/`](../edge_order_check/) for the check and [`../scale_test/`](../scale_test/) for runs with aligned weights. To fix, attach the weights as an igraph edge attribute keyed by the endpoint pair, or build the graph directly from the weight table's rows.

## Not included
The curated exosome network (node and edge lists), the resulting graph, and the feature table are not part of this repository.

## Citation
If you use this code, please cite the thesis and the dataset DOI above.
