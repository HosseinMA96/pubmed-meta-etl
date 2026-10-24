"""MeSH aggregation pilot (new analysis, not in the thesis).

Compares three ways to aggregate the term-pair MeSH similarity of two articles:
the median used for f7, the mean, and a symmetric best-match average. Runs on all
pairs of the pilot articles (one CEN cluster). The term-pair similarity is the one
of calculate_mesh_pair_similarity in hm31_graphs/step_7_calculate_similarities.

Usage: python3 -I mesh_pilot.py config.json results.json
"""
import ast
import json
import os
import sys
import time

import numpy as np


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def pair_similarity(tree1, tree2):
    """Number of shared leading components, counting the leading letter as one."""
    a = (tree1[0] + "." + tree1[1:]).split(".")
    b = (tree2[0] + "." + tree2[1:]).split(".")
    common = 0
    for x, y in zip(a, b):
        if x != y:
            break
        common += 1
    return common


def describe(x):
    q = np.quantile(x, [0.25, 0.5, 0.75])
    return {"q1": float(q[0]), "median": float(q[1]), "q3": float(q[2]), "mean": float(np.mean(x))}


def main(config_path, out_path):
    base = os.path.dirname(os.path.abspath(config_path))
    cfg = json.load(open(config_path))
    lookup = json.load(open(os.path.join(base, cfg["mesh_lookup"])))
    meta = json.load(open(os.path.join(base, cfg["metadata"])))

    articles = {}
    for pmid, rec in meta.items():
        rec = ast.literal_eval(rec) if isinstance(rec, str) else rec
        if rec.get("mesh"):
            articles[pmid] = rec["mesh"]
    all_terms = [t for terms in articles.values() for t in terms]
    missing = sum(t not in lookup for t in all_terms)
    log(f"articles with MeSH: {len(articles)}; term occurrences: {len(all_terms)}; missing from lookup: {missing}")

    # Index the distinct tree numbers and precompute their pairwise similarity
    trees = sorted({lookup[t][0] for t in all_terms if t in lookup})
    index = {tr: i for i, tr in enumerate(trees)}
    sim = np.zeros((len(trees), len(trees)), dtype=np.int8)
    for i, a in enumerate(trees):
        for j in range(i, len(trees)):
            sim[i, j] = sim[j, i] = pair_similarity(a, trees[j])
    log(f"distinct tree numbers: {len(trees)}")

    idx = [np.array([index[lookup[t][0]] for t in terms if t in lookup]) for terms in articles.values()]
    idx = [a for a in idx if a.size]
    n = len(idx)
    med, mean, best, cross = [], [], [], []
    for i in range(n):
        for j in range(i + 1, n):
            s = sim[np.ix_(idx[i], idx[j])]
            med.append(np.median(s))
            mean.append(s.mean())
            best.append((s.max(axis=1).mean() + s.max(axis=0).mean()) / 2)
            cross.append(np.mean(s == 0))
    med, mean, best, cross = map(np.asarray, (med, mean, best, cross))
    log(f"pairs: {med.size}")

    res = {"articles": n, "term_occurrences": len(all_terms), "terms_missing_from_lookup": missing,
           "pairs": int(med.size),
           "share_cross_category_term_pairs_mean": float(cross.mean()),
           "share_pairs_majority_cross_category": float(np.mean(cross > 0.5)),
           "nonzero_share": {"median": float(np.mean(med > 0)), "mean": float(np.mean(mean > 0)),
                             "best_match": float(np.mean(best > 0))},
           "distribution": {"median": describe(med), "mean": describe(mean), "best_match": describe(best)},
           "note": cfg["note"]}
    json.dump(res, open(out_path, "w"), indent=2)
    log(f"wrote {out_path}")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
