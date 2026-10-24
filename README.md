# pubmed-meta-etl

Parse the [PubMed baseline](https://pubmed.ncbi.nlm.nih.gov/download/#annual-baseline) XML into a table that says, for every paper with a DOI, **which metadata it has**: year, MeSH terms, title, abstract.

> **Status: early / alpha.** This is the original research pipeline, now being turned into a reusable, database-free tool. It is **not yet packaged or tested**; it is MIT-licensed (see [License](#license)). The roadmap is below.

## What it produces

One row per paper (unique PMID and unique, lower-cased DOI):

| column | meaning |
|---|---|
| `pmid` | PubMed ID |
| `doi` | DOI, lower-cased |
| `has_year`, `has_mesh`, `has_title`, `has_abstract` | `1` if the field is present in the PubMed record, otherwise `0` |

This is the table behind the `baseline2024.csv.gz` file of the Illinois Data Bank dataset
[Parsed Open Citations and PubMed Data](https://doi.org/10.13012/B2IDB-5216575_V1) (CC BY).
If you use the data or code, please cite that dataset DOI. The use case: given a large citation graph, keep only the papers that have the metadata you need.

### Rules as implemented today

1. Every `PubmedArticle` in every XML file is parsed into a record (PMID, DOI, year, MeSH, title, abstract, date revised, and a few more fields).
2. Duplicate DOIs are dropped, then only one record per PMID is kept (the one with the latest `date_revised`), then records with an empty DOI are dropped.
3. The flags are set from the cleaned fields.

Caveat: step 2 drops duplicate DOIs *before* choosing the latest revision, so which of two records sharing a DOI survives is not guaranteed to be the latest one. Behavior is kept as is for now so results stay comparable to the published dataset; see the roadmap.

## Quickstart (verified on a 2-file sample)

Tested on Linux with Python 3.10, Java 11, `pandas 2.3.3`, `pyarrow 25.0.1`, `xmltodict 1.0.4`, `pyspark 3.5.1` (with `py4j 0.10.9.7`). The original pins in `requirments.txt` are for Python 3.6 and are kept only for reference.

```sh
python3 -m venv venv && source venv/bin/activate
pip install pandas pyarrow xmltodict "pyspark==3.5.1" "py4j==0.10.9.7"

# 1. download two baseline files (about 37 MB) and check them against NCBI's .md5 files
mkdir -p data/gz data/parquet
for f in pubmed26n0001 pubmed26n0002; do
  curl -O --output-dir data/gz https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/$f.xml.gz
  curl -O --output-dir data/gz https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/$f.xml.gz.md5
done

# 2. unzip (this deletes the .gz files, so keep a copy), parse to Parquet, then dedupe and flag
python extractor.py -gz data/gz -cores 2
python parallel.py -xml data/gz -parquet data/parquet -cores 2 -wrap 0
env -u SPARK_HOME python pyspark_parquet.py --parquet --path data/parquet --dump_path data/out
```

On this sample the run takes under a minute on 2 cores and gives:

| | |
|---|---|
| records parsed | 60,000 (26,600 without a DOI) |
| rows in the final table | **33,400** |
| `has_title` / `has_mesh` / `has_year` | 33,400 / 33,400 / 33,400 |
| `has_abstract` | 18,144 |

Reference numbers and an output checksum are in [`baseline/reference_stats.json`](baseline/reference_stats.json); `baseline/baseline_stats.py data_dir` recomputes them (`data_dir` must contain `parquet/` and `out/`). These are from the 2026 baseline files above, **not** from the 2024 baseline used for the published dataset.

**Troubleshooting.** If Spark fails with `py4j ... Method sql(...) does not exist`, a `SPARK_HOME` from another Spark installation is overriding the one in your virtualenv. Unset it, as in the command above.

### Optional: write to Postgres instead of Parquet

`pubmed_etl.sh` runs the whole pipeline and writes to a Postgres table. It reads its settings from environment variables, and nothing is stored in the repository:

```sh
export PUBMED_ETL_GZ_INPUT=... PUBMED_ETL_PARQUET_OUTPUT=... PUBMED_ETL_DB_USER=... PUBMED_ETL_DB_PASSWORD=...
export PUBMED_ETL_JDBC_URL=jdbc:postgresql://HOST:5432/DATABASE   # also PUBMED_ETL_TABLE, PUBMED_ETL_CORES
./pubmed_etl.sh
```

It needs the Postgres JDBC jar passed to `spark-submit`; the jar in this repo is version 42.5.2. This route has not been re-tested yet. See `PubMed ETL guideline.pdf` for the original instructions.

## Scale

The full 2026 baseline directory has 1,334 files and about 52 GB gzipped. Expanded XML is roughly ten times larger (195 MB and 183 MB for the two sample files above). Full-run time and memory have **not** been measured with the current code.

## Known issues

- A record without a journal title can make the parser fail on its whole XML file (`journal_title` is never set).
- `parallel.py` uses the `-cores` value where `-wrap` is meant, and the help texts of those two options are swapped.
- The `postgres` mode inside `parallel.py` refers to a field that is never produced and does not work; use the Spark step for Postgres.
- `pubmed_etl.sh` computes the data-handling time from an undefined variable.
- The year is the first four-digit number in the publication date text.
- The parser reads each XML file fully into memory and hides most parse errors.

## Roadmap

Goal: a pip-installable tool that runs **locally without a database or Spark** on a small sample, and also scales to the full baseline with a configuration switch.

1. Package layout, config file, command-line interface, limited `download` command.
2. Local parser that reproduces the reference numbers above exactly; tests and CI.
3. Full-scale mode (multiprocessing, optional Spark), with measured timings.
4. A `prune` command that filters a citation edge list (for example the OpenCitations edge list in the dataset above) to papers that have the metadata you choose, and a coverage report.
5. Documentation, contribution guidelines, tagged releases.

Progress is tracked in the issues and in [CHANGELOG.md](CHANGELOG.md).

## Thesis pipeline (graph join, features, clustering)

[`thesis_pipeline/`](thesis_pipeline/) holds the research code that used this table in the M.S. thesis *A Framework for Similarity-Based Clustering of Biomedical Citation Graphs* (UIUC, 2024). The code joins a citation graph to the PubMed table by DOI, keeps the papers with the metadata you need, computes seven edge-similarity features (bibliographic coupling and co-citation counts and Jaccard coefficients, publication year, MeSH, and MedCPT embedding similarity), combines them with user weights, and clusters the graph with Leiden-CPM. It is a cleaned copy of [illinois-or-research-analytics/hm31_graphs](https://github.com/illinois-or-research-analytics/hm31_graphs) with credentials removed, and it is not packaged or tested in this form; see its README. The citation graph used in the thesis is not included.

## Provenance

This repository continues the development of
[illinois-or-research-analytics/pubmed_etl](https://github.com/illinois-or-research-analytics/pubmed_etl)
and keeps its full commit history, with the original authors and dates. The commit hashes differ from the original because a database password was redacted from two historical commits before this repository was published. The original repository is unchanged.

## License

MIT; see [LICENSE](LICENSE).

## Authors and contact

Created by Hossein Mohasel Arjomandi at the University of Illinois Urbana-Champaign. Full author list and citation file to follow. `TODO(authors)`

Questions and bug reports: please open an issue.

## Development notes

Parts of the documentation and refactoring in this repository are prepared with AI assistance (Claude Code) and reviewed by the author. Commits made with AI help carry a `Co-Authored-By` line.
