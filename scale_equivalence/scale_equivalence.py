"""Sanity check of the scale identity used in Paper B.

CPM quality (thesis Eq. 3.1):  H(P; w, gamma) = sum_c ( e_c(w) - gamma * C(n_c, 2) ),
where e_c(w) is the total weight of edges inside cluster c.

Claim: H(P; alpha*w, gamma) = alpha * H(P; w, gamma / alpha) for every partition P, alpha > 0.
Hence the optimal partition for (alpha*w, gamma) equals the optimal partition for (w, gamma/alpha).

Part 1 checks the identity on random partitions of a random weighted graph.
Part 2 enumerates all set partitions of a small graph and compares the argmax partitions.
This tests the quality function and its optimum, not the Leiden heuristic itself.
Run: python3 -I scale_equivalence.py
"""
import json
import itertools
import numpy as np

cfg = json.load(open("config.json"))
rng = np.random.default_rng(cfg["seed"])


def make_graph(n, p):
    edges = [(i, j) for i in range(n) for j in range(i + 1, n) if rng.random() < p]
    w = rng.random(len(edges))
    return edges, w


def cpm(edges, w, labels, gamma):
    labels = np.asarray(labels)
    inside = {}
    for (i, j), wt in zip(edges, w):
        if labels[i] == labels[j]:
            inside[labels[i]] = inside.get(labels[i], 0.0) + wt
    total = 0.0
    for c in np.unique(labels):
        n_c = int((labels == c).sum())
        total += inside.get(c, 0.0) - gamma * n_c * (n_c - 1) / 2
    return total


def set_partitions(n):
    """Restricted growth strings: all set partitions of range(n)."""
    def rec(i, cur, mx):
        if i == n:
            yield tuple(cur)
            return
        for v in range(mx + 2):
            cur.append(v)
            yield from rec(i + 1, cur, max(mx, v))
            cur.pop()
    yield from rec(1, [0], 0)


# Part 1: identity on random partitions
edges, w = make_graph(cfg["n_nodes_identity"], cfg["edge_prob"])
max_err = 0.0
checks = 0
for _ in range(cfg["n_random_partitions"]):
    k = int(rng.integers(2, 12))
    labels = rng.integers(0, k, size=cfg["n_nodes_identity"])
    for a in cfg["alphas"]:
        for g in cfg["gammas"]:
            lhs = cpm(edges, a * w, labels, g)
            rhs = a * cpm(edges, w, labels, g / a)
            max_err = max(max_err, abs(lhs - rhs))
            checks += 1
print(f"Part 1: {checks} checks on a {cfg['n_nodes_identity']}-node graph with {len(edges)} edges; max |lhs-rhs| = {max_err:.3e}")
assert max_err < cfg["tolerance"], "identity violated"

# Part 2: argmax partitions on all set partitions of a small graph
n = cfg["n_nodes_bruteforce"]
edges2, w2 = make_graph(n, cfg["edge_prob_bruteforce"])
parts = list(set_partitions(n))
print(f"Part 2: {len(parts)} set partitions of {n} nodes, {len(edges2)} edges")
ok = True
for a in cfg["alphas"]:
    for g in cfg["gammas"]:
        best_a = max(parts, key=lambda P: cpm(edges2, a * w2, P, g))
        best_b = max(parts, key=lambda P: cpm(edges2, w2, P, g / a))
        va = cpm(edges2, a * w2, best_a, g)
        vb = a * cpm(edges2, w2, best_b, g / a)
        same = abs(va - vb) < cfg["tolerance"] and cpm(edges2, w2, best_a, g / a) >= cpm(edges2, w2, best_b, g / a) - cfg["tolerance"]
        ok &= same
        print(f"  alpha={a:5.1f} gamma={g:4.2f}: best quality {va:.6f} vs {vb:.6f}; same optimum: {same}")
assert ok, "optima differ"
print("All checks passed.")
