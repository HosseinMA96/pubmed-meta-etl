import sys, glob, hashlib, json
import pandas as pd
b = sys.argv[1]
raw = pd.concat([pd.read_parquet(f).reset_index() for f in sorted(glob.glob(b + '/parquet/*.parquet'))])
out = pd.read_parquet(glob.glob(b + '/out/*.parquet')[0]).sort_values('pmid').reset_index(drop=True)
stats = {
  'raw_rows': len(raw), 'raw_unique_pmid': int(raw.PMID.nunique()), 'raw_empty_doi': int((raw.doi == '').sum()),
  'raw_unique_nonempty_doi': int(raw[raw.doi != ''].doi.nunique()),
  'out_rows': len(out), 'out_unique_pmid': int(out.pmid.nunique()), 'out_unique_doi': int(out.doi.nunique()),
  'sum_has_abstract': int(out.has_abstract.sum()), 'sum_has_title': int(out.has_title.sum()),
  'sum_has_mesh': int(out.has_mesh.sum()), 'sum_has_year': int(out.has_year.sum()),
  'out_columns': list(out.columns), 'out_dtypes': {k: str(v) for k, v in out.dtypes.items()},
  'out_sha256_sorted_csv': hashlib.sha256(out.to_csv(index=False).encode()).hexdigest(),
}
print(json.dumps(stats, indent=1))
