"""Audit of the CENM edge-feature table (new analysis, not in the thesis).

Reads the raw feature table column by column and reports counts, ranges,
distributions, Spearman correlations, an agreement check between topological
and semantic features, and the [1,10] weight mapping used in thesis Sec. 4.4.

Usage: python3 -I feature_audit.py config.json results.json
"""
import csv
import json
import os
import sys
import time

import numpy as np
import pyarrow.parquet as pq
from scipy.stats import spearmanr

# Paper notation -> column in the parquet file
FEATURES = {
    "f1_bib_count": "bib_coupling_frequency_similarity",
    "f2_bib_jaccard": "bib_coupling_jaccard_similarity",
    "f3_cocit_count": "cocitation_frequency_similarity",
    "f4_cocit_jaccard": "cocitation_jaccard_similarity",
    "f5_year": "year_similarity",
    "f6_cosine": "cosine_similarity",
    "f7_mesh_median": "mesh_similarity_median",
}
SCALED = ["f1_bib_count", "f2_bib_jaccard", "f3_cocit_count", "f4_cocit_jaccard", "f7_mesh_median"]
TOPOLOGICAL = ["f1_bib_count", "f2_bib_jaccard", "f3_cocit_count", "f4_cocit_jaccard"]


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def load_column(pf, name):
    """Return (values as float64 with nulls filled by 0, number of nulls)."""
    col = pf.read(columns=[name]).column(0)
    nulls = col.null_count
    arr = col.to_numpy(zero_copy_only=False)
    arr = np.nan_to_num(arr.astype(np.float64), nan=0.0)
    return arr, nulls


def describe(x):
    q = np.quantile(x, [0.25, 0.5, 0.75, 0.99])
    return {"min": float(x.min()), "q1": float(q[0]), "median": float(q[1]), "mean": float(x.mean()),
            "q3": float(q[2]), "p99": float(q[3]), "max": float(x.max())}


def main(config_path, out_path):
    cfg = json.load(open(config_path))
    pf = pq.ParquetFile(cfg["parquet"])
    n = pf.metadata.num_rows
    log(f"rows: {n}")
    res = {"rows": n, "features": {}}

    rng = np.random.default_rng(cfg["seed"])
    sample_idx = np.sort(rng.choice(n, size=cfg["spearman_sample"], replace=False))
    sample = {}
    scaled_topo_sum = np.zeros(n, dtype=np.float64)
    keep = {}  # full columns needed later for the agreement check

    for key, colname in FEATURES.items():
        x, nulls = load_column(pf, colname)
        info = {"column": colname, "nulls": int(nulls), "non_null": int(n - nulls),
                "nonzero": int(np.count_nonzero(x)), "nonzero_share": float(np.count_nonzero(x) / n),
                "raw_all_edges": describe(x)}
        nz = x[x != 0]
        info["raw_nonzero_edges"] = describe(nz) if nz.size else None
        if key == "f5_year":
            vals, counts = np.unique(x, return_counts=True)
            info["value_counts"] = {str(v): int(c) for v, c in zip(vals, counts)}
        if key == "f6_cosine":
            info["exactly_0.5"] = int(np.count_nonzero(x == 0.5))
            info["below_0.5"] = int(np.count_nonzero(x < 0.5))
            info["negative"] = int(np.count_nonzero(x < 0))
            info["exactly_1"] = int(np.count_nonzero(x == 1.0))
            info["at_least_0.99"] = int(np.count_nonzero(x >= 0.99))
        if key in SCALED:
            lo, hi = x.min(), x.max()
            s = (x - lo) / (hi - lo)  # Spark MinMaxScaler with default range [0,1]
            info["scaling_min_is_zero"] = bool(lo == 0)  # then min-max == divide by max
            info["scaled_all_edges"] = describe(s)
            info["scaled_share_below_0.01_of_nonzero"] = float(np.count_nonzero((s > 0) & (s < 0.01)) / max(nz.size, 1))
            if key in TOPOLOGICAL:
                scaled_topo_sum += s
            x_for_sample = s
        else:
            x_for_sample = x
        sample[key] = x_for_sample[sample_idx]
        if key in ("f1_bib_count", "f2_bib_jaccard", "f6_cosine", "f7_mesh_median", "f5_year"):
            keep[key] = x
        res["features"][key] = info
        log(f"{key}: done")
        del x, nz

    # Thesis Table 4.3: equal weights (1/4) on the four scaled topological features
    w = scaled_topo_sum / 4.0
    del scaled_topo_sum
    nzw = w[w > 0]
    res["topological_equal_weight"] = {"nonzero_edges": int(nzw.size), "nonzero_share": float(nzw.size / n),
                                       "nonzero_stats": describe(nzw)}
    # Thesis Sec. 4.4 map: zero edges -> 1, nonzero mapped linearly to [1,10]
    wmin, wmax = nzw.min(), nzw.max()
    mapped = np.where(w > 0, (w - wmin) * 9.0 / (wmax - wmin) + 1.0, 1.0)
    res["map_1_10_equal_weight"] = {"slope": float(9.0 / (wmax - wmin)), "share_exactly_1": float(np.mean(mapped == 1.0)),
                                    "mean_all_edges": float(mapped.mean()), "mean_nonzero_edges": float(mapped[w > 0].mean()),
                                    "stats_all_edges": describe(mapped)}
    zero_topo = w == 0
    del mapped, nzw
    log("weights: done")

    # Spearman correlations on the fixed-seed sample (scaled features where scaling applies)
    keys = list(FEATURES)
    rho, _ = spearmanr(np.column_stack([sample[k] for k in keys]))
    res["spearman_sample"] = {"n": int(cfg["spearman_sample"]), "seed": cfg["seed"], "order": keys,
                              "matrix": [[round(float(v), 4) for v in row] for row in rho]}
    log("spearman: done")

    # Agreement check: semantic features for zero-topology edges vs bib-Jaccard quantile bins
    bj = keep["f2_bib_jaccard"]
    groups = {"zero_topological": zero_topo}
    pos = bj > 0
    edges_q = np.quantile(bj[pos], np.linspace(0, 1, cfg["agreement_bins"] + 1))
    for i in range(cfg["agreement_bins"]):
        lo, hi = edges_q[i], edges_q[i + 1]
        m = pos & (bj >= lo) & ((bj <= hi) if i == cfg["agreement_bins"] - 1 else (bj < hi))
        groups[f"bib_jaccard_q{i + 1}[{lo:.4g},{hi:.4g}]"] = m
    agree = {}
    for g, m in groups.items():
        agree[g] = {"edges": int(m.sum()),
                    "f6_mean": float(keep["f6_cosine"][m].mean()), "f6_median": float(np.median(keep["f6_cosine"][m])),
                    "f6_std": float(keep["f6_cosine"][m].std()),
                    "f7_raw_mean": float(keep["f7_mesh_median"][m].mean()),
                    "f5_share_within_5y": float(np.mean(keep["f5_year"][m] == 1.0))}
    res["agreement_by_bib_jaccard"] = agree
    # Effect size of the f6 difference between the zero-overlap group and the top quintile
    g0, g5 = agree["zero_topological"], agree[list(agree)[-1]]
    pooled = np.sqrt((g0["f6_std"] ** 2 + g5["f6_std"] ** 2) / 2)
    f6q = res["features"]["f6_cosine"]["raw_all_edges"]
    diff = g5["f6_mean"] - g0["f6_mean"]
    res["agreement_effect_f6"] = {"difference": diff, "cohens_d": float(diff / pooled),
                                  "difference_over_iqr": float(diff / (f6q["q3"] - f6q["q1"]))}
    log("agreement: done")

    # Near-identical texts (possible republications or duplicate records)
    near = keep["f6_cosine"] >= 0.99
    top = np.argmax(keep["f6_cosine"])
    res["near_identical_text"] = {"edges_f6_ge_0.99": int(near.sum()),
                                  "within_5y": int((near & (keep["f5_year"] == 1.0)).sum()),
                                  "bib_jaccard_ge_0.5": int((near & (bj >= 0.5)).sum()),
                                  "bib_jaccard_gt_0": int((near & (bj > 0)).sum()),
                                  "max_f6_edge": {"f6": float(keep["f6_cosine"][top]), "bib_jaccard": float(bj[top]), "bib_count": float(keep["f1_bib_count"][top]),
                                                  "f5": float(keep["f5_year"][top])}}

    # Missing-embedding nodes: every incident edge has f6 = 0.5 (rule ii). Check they are
    # disjoint from the near-identical edges.
    ends = pf.read(columns=["first", "second"])
    u, v = ends.column(0).to_numpy(), ends.column(1).to_numpy()
    del ends
    is_half = keep["f6_cosine"] == 0.5
    deg = np.bincount(np.concatenate([u, v]))
    deg_half = np.bincount(np.concatenate([u, v]), weights=np.concatenate([is_half, is_half]), minlength=deg.size)
    missing = (deg > 0) & (deg_half == deg)
    touches = missing[u] | missing[v]
    res["missing_embedding_nodes"] = {"nodes_all_edges_0.5": int(missing.sum()),
                                      "edges_touching_them": int(touches.sum()),
                                      "edges_0.5_not_touching_them": int((is_half & ~touches).sum()),
                                      "overlap_with_f6_ge_0.99_edges": int((touches & near).sum())}
    del u, v, touches
    log("missing-embedding check: done")

    # Compare with the numbers already reported in the papers / thesis
    exp = cfg["expected"]
    f = res["features"]
    res["checks"] = {
        "edges": [exp["edges"], n],
        "bib_rows": [exp["bib_rows"], f["f1_bib_count"]["non_null"]],
        "cocitation_rows": [exp["cocitation_rows"], f["f3_cocit_count"]["non_null"]],
        "nonzero_topological": [exp["nonzero_topological"], res["topological_equal_weight"]["nonzero_edges"]],
        "table_4_3": [exp["table_4_3"], res["topological_equal_weight"]["nonzero_stats"]],
    }
    json.dump(res, open(out_path, "w"), indent=2)
    log(f"wrote {out_path}")
    write_csvs(res, os.path.dirname(os.path.abspath(out_path)))


def write_csvs(res, out_dir):
    """Write the summaries behind Tables IV and V and the Spearman matrix as CSV."""
    with open(os.path.join(out_dir, "feature_distributions.csv"), "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["feature", "column", "nonzero_edges", "nonzero_share", "median_nonzero", "p99_nonzero",
                    "max", "scaled_share_below_0.01_of_nonzero"])
        for key, info in res["features"].items():
            nz = info["raw_nonzero_edges"]
            w.writerow([key, info["column"], info["nonzero"], info["nonzero_share"], nz["median"], nz["p99"],
                        nz["max"], info.get("scaled_share_below_0.01_of_nonzero", "")])
    with open(os.path.join(out_dir, "agreement_by_bib_jaccard.csv"), "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["group", "edges", "f6_mean", "f6_median", "f6_std", "f7_raw_mean", "f5_share_within_5y"])
        for g, a in res["agreement_by_bib_jaccard"].items():
            w.writerow([g, a["edges"], a["f6_mean"], a["f6_median"], a["f6_std"], a["f7_raw_mean"], a["f5_share_within_5y"]])
    sp = res["spearman_sample"]
    with open(os.path.join(out_dir, "spearman_sample.csv"), "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow([""] + sp["order"])
        for k, row in zip(sp["order"], sp["matrix"]):
            w.writerow([k] + row)
    log(f"wrote CSV summaries to {out_dir}")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
