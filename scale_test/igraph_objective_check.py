"""Checks which CPM objective igraph's community_leiden optimizes (new check for Paper B, round-2 review item 3).

igraph docs: Q = 1/(2m) sum_ij (A_ij - gamma n_i n_j) delta(s_i, s_j), sum over all ordered pairs including i = j.
With unit node weights this is sum_c (e_c - gamma n_c^2 / 2) up to the factor 1/m, i.e. the leidenalg / Eq. (2) form
sum_c (e_c - gamma C(n_c, 2)) minus the partition-independent constant gamma N / 2. So both have the same optimal
partitions. This script checks the identity on random graphs for unweighted and weighted edges.
"""
import random

import igraph as ig
import numpy as np


def h_binom(g, memb, gamma, w):
    memb = np.asarray(memb)
    n_c = np.bincount(memb)
    u, v = np.array(g.get_edgelist()).T
    intra = memb[u] == memb[v]
    e_c = np.bincount(memb[u][intra], weights=w[intra], minlength=n_c.size)
    return float((e_c - gamma * n_c * (n_c - 1) / 2).sum())


rng = np.random.default_rng(20261024)
print("graph  weighted  gamma  igraph_quality*m  H_binom  H_binom-gamma*N/2  diff")
for trial in range(6):
    random.seed(trial)
    g = ig.Graph.Erdos_Renyi(400, 0.02 + 0.01 * trial)
    for weighted in (False, True):
        w = rng.random(g.ecount()) if weighted else np.ones(g.ecount())
        for gamma in (0.01, 0.05, 0.25):
            random.seed(trial)
            p = g.community_leiden(objective_function="CPM", weights=w.tolist() if weighted else None,
                                   resolution=gamma, n_iterations=-1)
            # igraph normalizes the quality by the total edge weight (m for unweighted graphs)
            q = p.quality * w.sum()
            h = h_binom(g, p.membership, gamma, w)
            c = h - gamma * g.vcount() / 2
            print(f"{trial}  {weighted!s:5}  {gamma:<5}  {q:14.6f}  {h:12.6f}  {c:12.6f}  {q - c:+.2e}")
