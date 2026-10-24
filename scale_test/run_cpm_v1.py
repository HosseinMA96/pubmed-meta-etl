"""Leiden-CPM on CENM with thesis weight settings (new experiments, not in the thesis).

Builds the edge weight of a named setting with the thesis formula
(hm31_graphs/step_10_cluster/transformation.py: calculate_weight), optionally
divides it by its mean or maps it to [1,10] as in the thesis, runs Leiden-CPM,
and records coverage, the thesis CPM score, and intra-cluster coherence.

Usage: python3 -I run_cpm.py config.json prep       (writes the edge files once)
       python3 -I run_cpm.py config.json RUN_NAME   (writes runs/RUN_NAME.json)
Run each job under a memory cap, e.g. systemd-run --user --scope -p MemoryMax=10G -p MemorySwapMax=0.
"""
import pyarrow as pa
import pyarrow.csv as pacsv
import json
import os
import random
import resource
import sys
import time

import igraph as ig
import leidenalg as la
import numpy as np
import pyarrow.parquet as pq

# Thesis order of the weight vector (transformation.py: calculate_weight)
ORDER = ["year_similarity", "mesh_similarity_median", "cocitation_jaccard_similarity",
         "cocitation_frequency_similarity", "bib_coupling_jaccard_similarity",
         "bib_coupling_frequency_similarity", "cosine_similarity"]
SCALED = {"mesh_similarity_median", "cocitation_jaccard_similarity", "cocitation_frequency_similarity",
          "bib_coupling_jaccard_similarity", "bib_coupling_frequency_similarity"}


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def feature(pf, name):
    """Raw feature with nulls set to 0; scaled features divided by their max (min is 0)."""
    x = pf.read(columns=[name]).column(0).to_numpy(zero_copy_only=False).astype(np.float64)
    x = np.nan_to_num(x, nan=0.0)
    if name in SCALED:
        x /= x.max()
    return x


def build_weight(pf, coeffs, transform):
    w = np.zeros(pf.metadata.num_rows, dtype=np.float64)
    for name, c in zip(ORDER, coeffs):
        if c:
            w += c * feature(pf, name)
    mean_raw = float(w.mean())
    if transform == "mean_norm":
        w /= mean_raw
    elif transform == "map110":
        # Thesis interval_mapping_preprocessing + apply_linear_transformation
        second_min = w[w > w.min()].min()
        w[w == 0] = second_min
        w = (w - second_min) * 9.0 / (w.max() - second_min) + 1.0
    return w, mean_raw


def prepare_edges(cfg):
    """Remap node IDs to 0..n-1 and write the edge list (parquet row order) as text and .npy."""
    pf = pq.ParquetFile(cfg["parquet"])
    ends = pf.read(columns=["first", "second"])
    m = pf.metadata.num_rows
    ids, inv = np.unique(np.concatenate([ends.column(0).to_numpy(), ends.column(1).to_numpy()]), return_inverse=True)
    del ends
    inv = inv.astype(np.int32)
    os.makedirs(cfg["work_dir"], exist_ok=True)
    np.save(os.path.join(cfg["work_dir"], "u.npy"), inv[:m])
    np.save(os.path.join(cfg["work_dir"], "v.npy"), inv[m:])
    table = pa.table({"u": inv[:m], "v": inv[m:]})
    pacsv.write_csv(table, os.path.join(cfg["work_dir"], "edges.txt"),
                    pacsv.WriteOptions(include_header=False, delimiter=" "))
    log(f"prepared {ids.size} nodes, {m} edges")


def main(config_path, run_name):
    base = os.path.dirname(os.path.abspath(config_path))
    cfg = json.load(open(config_path))
    run = cfg["runs"][run_name]
    setting = cfg["settings"][run["setting"]]
    gamma, seed, transform = run.get("gamma", cfg["gamma"]), run["seed"], run.get("transform", "raw")
    pf = pq.ParquetFile(cfg["parquet"])
    t0 = time.time()

    m = pf.metadata.num_rows
    g = ig.Graph.Read_Edgelist(os.path.join(cfg["work_dir"], "edges.txt"), directed=False)
    n = g.vcount()
    assert g.ecount() == m
    log(f"graph: {n} nodes, {m} edges ({time.time() - t0:.0f}s)")

    weight_list, mean_raw = None, 1.0
    if setting is not None:
        weights, mean_raw = build_weight(pf, setting, transform)
        log(f"weights: mean raw {mean_raw:.6g}, mean used {weights.mean():.6g}")
        weight_list = weights.tolist()
        del weights

    t1 = time.time()
    engine = cfg.get("engine", "leidenalg")
    if engine == "igraph":
        # igraph's C implementation of Leiden; python-igraph draws from Python's random module
        random.seed(seed)
        part = g.community_leiden(objective_function="CPM", weights=weight_list, resolution=gamma,
                                  n_iterations=2)
    else:
        part = la.find_partition(g, la.CPMVertexPartition, weights=weight_list,
                                 resolution_parameter=gamma, seed=seed)
    t_cluster = time.time() - t1
    memb = np.asarray(part.membership, dtype=np.int32)
    log(f"clustered in {t_cluster:.0f}s")
    del part, g, weight_list

    u = np.load(os.path.join(cfg["work_dir"], "u.npy"))
    v = np.load(os.path.join(cfg["work_dir"], "v.npy"))
    weights = build_weight(pf, setting, transform)[0] if setting is not None else None

    size = np.bincount(memb)
    pairs = size * (size - 1) / 2.0
    intra = memb[u] == memb[v]
    e_c = np.bincount(memb[u][intra], minlength=size.size)
    res = {"run": run_name, "engine": engine, "setting": run["setting"], "coeffs_thesis_order": setting, "transform": transform,
           "gamma": gamma, "seed": seed, "nodes": int(n), "edges": int(m),
           "mean_weight_raw": mean_raw, "predicted_effective_gamma": gamma / mean_raw if setting else gamma,
           "cov1": float(size[memb][size[memb] > 1].size / n * 100),
           "cov10": float(size[memb][size[memb] > 10].size / n * 100),
           "clusters_gt1": int((size > 1).sum()), "clusters_gt10": int((size > 10).sum()),
           "singletons": int((size == 1).sum()), "max_size": int(size.max()),
           # Thesis CPM score: unweighted e_c, gamma 0.05, singletons contribute 0
           "cpm_score_unweighted_0.05": float((e_c - 0.05 * pairs).sum()),
           "intra_edge_share": float(intra.mean()), "cluster_seconds": t_cluster}
    if weights is not None:
        w_c = np.bincount(memb[u][intra], weights=weights[intra], minlength=size.size)
        res["weighted_H"] = float((w_c - gamma * pairs).sum())
    del weights

    # Coherence over intra-cluster edges
    f6 = pf.read(columns=["cosine_similarity"]).column(0).to_numpy()
    f5 = pf.read(columns=["year_similarity"]).column(0).to_numpy()
    f2 = np.nan_to_num(pf.read(columns=["bib_coupling_jaccard_similarity"]).column(0)
                       .to_numpy(zero_copy_only=False).astype(np.float64), nan=0.0)
    res["coherence_intra"] = {"mean_f6": float(f6[intra].mean()), "share_within_5y": float((f5[intra] == 1.0).mean()),
                              "mean_bib_jaccard": float(f2[intra].mean())}
    res["coherence_all_edges"] = {"mean_f6": float(f6.mean()), "share_within_5y": float((f5 == 1.0).mean()),
                                  "mean_bib_jaccard": float(f2.mean())}
    res["peak_rss_gb"] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1e6
    res["total_seconds"] = time.time() - t0
    os.makedirs(os.path.join(base, "runs"), exist_ok=True)
    json.dump(res, open(os.path.join(base, "runs", f"{run_name}.json"), "w"), indent=2)
    log(f"cov1 {res['cov1']:.2f} cov10 {res['cov10']:.2f} cpm {res['cpm_score_unweighted_0.05']:.4g} "
        f"rss {res['peak_rss_gb']:.1f} GB")


if __name__ == "__main__":
    if sys.argv[2] == "prep":
        prepare_edges(json.load(open(sys.argv[1])))
    else:
        main(sys.argv[1], sys.argv[2])
