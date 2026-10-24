"""Leiden-CPM on CENM with thesis weight settings (new experiments, not in the thesis).

Builds the edge weight of a named setting with the thesis formula
(hm31_graphs/step_10_cluster/transformation.py: calculate_weight), optionally
divides it by its mean or maps it to [1,10] as in the thesis, runs Leiden-CPM,
and records coverage, the thesis CPM score, and intra-cluster coherence.

Transforms: raw, mean_norm (divide by mean), mean_norm_floor (zeros -> smallest positive weight, then divide by
mean; v3), map110 (thesis [1,10] map). Each JSON (v3) also stores start/end time, config entry, versions, host.

Option "misalign" (per run): "nx_order" gives row k's weight to the edge that the thesis graph
construction puts at position k (networkx iteration order, see experiments/edge_order_check);
"shuffle" gives the weights a seeded random permutation. Both test the effect of weights that are
not aligned with their edges.

Usage: python3 -I run_cpm.py config.json prep       (writes the edge files once)
       python3 -I run_cpm.py config.json prep_nx    (writes the thesis edge order once)
       python3 -I run_cpm.py config.json RUN_NAME   (writes runs/RUN_NAME.json)
Run each job under a memory cap, e.g. systemd-run --user --scope -p MemoryMax=10G -p MemorySwapMax=0.
"""
import pyarrow as pa
import pyarrow.csv as pacsv
import hashlib
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
    elif transform == "mean_norm_floor":
        # zero-weight edges raised to the smallest positive weight, then divided by the mean
        w[w == 0] = w[w > 0].min()
        w /= w.mean()
    elif transform == "map110":
        # Thesis interval_mapping_preprocessing + apply_linear_transformation
        second_min = w[w > w.min()].min()
        w[w == 0] = second_min
        w = (w - second_min) * 9.0 / (w.max() - second_min) + 1.0
    return w, mean_raw


def work_dir(cfg, base):
    return os.path.join(base, cfg["work_dir"])  # absolute paths are kept as they are


def prepare_nx_order(cfg, base):
    """Row index of the edge at each igraph position under the thesis construction.

    nx.from_pandas_edgelist adds nodes in order of first appearance (first, then second, row by
    row); G.edges() yields each edge at its endpoint that appears first, grouped by that endpoint
    and in row order within a group; relabeling and ig.Graph.from_networkx keep this order.
    Checked against the thesis functions on small graphs in experiments/edge_order_check."""
    wd = work_dir(cfg, base)
    u, v = np.load(os.path.join(wd, "u.npy")), np.load(os.path.join(wd, "v.npy"))
    seq = np.empty(2 * u.size, dtype=np.int32)
    seq[0::2], seq[1::2] = u, v
    _, first_idx, inv = np.unique(seq, return_index=True, return_inverse=True)
    del seq
    rank = np.empty(first_idx.size, dtype=np.int64)
    rank[np.argsort(first_idx)] = np.arange(first_idx.size)
    key = np.minimum(rank[inv[0::2]], rank[inv[1::2]])
    del inv, rank
    order = np.argsort(key, kind="stable").astype(np.int64)
    np.save(os.path.join(wd, "nx_order.npy"), order)
    log(f"nx order: {np.mean(order == np.arange(order.size)) * 100:.4f}% of edges keep their row position")


def misalign(w, mode, cfg, base):
    if mode == "nx_order":
        order = np.load(os.path.join(work_dir(cfg, base), "nx_order.npy"))
        out = np.empty_like(w)
        out[order] = w  # thesis: igraph edge k (= row order[k]) received weight row k
        return out
    if mode == "shuffle":
        return w[np.random.default_rng(cfg["shuffle_seed"]).permutation(w.size)]
    return w


def prepare_edges(cfg, base):
    """Remap node IDs to 0..n-1 and write the edge list (parquet row order) as text and .npy."""
    pf = pq.ParquetFile(cfg["parquet"])
    ends = pf.read(columns=["first", "second"])
    m = pf.metadata.num_rows
    ids, inv = np.unique(np.concatenate([ends.column(0).to_numpy(), ends.column(1).to_numpy()]), return_inverse=True)
    del ends
    inv = inv.astype(np.int32)
    os.makedirs(work_dir(cfg, base), exist_ok=True)
    np.save(os.path.join(work_dir(cfg, base), "u.npy"), inv[:m])
    np.save(os.path.join(work_dir(cfg, base), "v.npy"), inv[m:])
    table = pa.table({"u": inv[:m], "v": inv[m:]})
    pacsv.write_csv(table, os.path.join(work_dir(cfg, base), "edges.txt"),
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
    started_at = time.strftime("%Y-%m-%dT%H:%M:%S%z")

    m = pf.metadata.num_rows
    g = ig.Graph.Read_Edgelist(os.path.join(work_dir(cfg, base), "edges.txt"), directed=False)
    n = g.vcount()
    assert g.ecount() == m
    log(f"graph: {n} nodes, {m} edges ({time.time() - t0:.0f}s)")

    weight_list, mean_raw = None, 1.0
    mode = run.get("misalign")
    if setting is not None:
        weights, mean_raw = build_weight(pf, setting, transform)
        weights = misalign(weights, mode, cfg, base)
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

    u = np.load(os.path.join(work_dir(cfg, base), "u.npy"))
    v = np.load(os.path.join(work_dir(cfg, base), "v.npy"))
    weights = misalign(build_weight(pf, setting, transform)[0], mode, cfg, base) if setting is not None else None

    size = np.bincount(memb)
    pairs = size * (size - 1) / 2.0
    intra = memb[u] == memb[v]
    e_c = np.bincount(memb[u][intra], minlength=size.size)
    res = {"run": run_name, "engine": engine, "setting": run["setting"], "coeffs_thesis_order": setting, "transform": transform, "misalign": mode,
           "script_sha256": hashlib.sha256(open(__file__, "rb").read()).hexdigest(),
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
    res["started_at"], res["finished_at"] = started_at, time.strftime("%Y-%m-%dT%H:%M:%S%z")
    res["config_entry"] = run
    res["versions"] = {"python": sys.version.split()[0], "igraph": ig.__version__, "leidenalg": la.__version__,
                       "numpy": np.__version__, "pyarrow": pa.__version__}
    res["host"] = os.uname().nodename
    os.makedirs(os.path.join(base, "runs"), exist_ok=True)
    json.dump(res, open(os.path.join(base, "runs", f"{run_name}.json"), "w"), indent=2)
    log(f"cov1 {res['cov1']:.2f} cov10 {res['cov10']:.2f} cpm {res['cpm_score_unweighted_0.05']:.4g} "
        f"rss {res['peak_rss_gb']:.1f} GB")


if __name__ == "__main__":
    if sys.argv[2] == "prep":
        prepare_edges(json.load(open(sys.argv[1])), os.path.dirname(os.path.abspath(sys.argv[1])))
    elif sys.argv[2] == "prep_nx":
        prepare_nx_order(json.load(open(sys.argv[1])), os.path.dirname(os.path.abspath(sys.argv[1])))
    else:
        main(sys.argv[1], sys.argv[2])
