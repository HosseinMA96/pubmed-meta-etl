"""Does the thesis graph construction keep the row order of the edge table? (new check, not in the thesis)

The thesis builds the igraph graph with construct_base_graph.py:construct_graph
(nx.from_pandas_edgelist -> relabel_networkx_nodes -> ig.Graph.from_networkx) and later passes
the weight column of a separate table ordered by "id" to leidenalg by position
(weighting_clustering_pipeline.py:leiden, weights=pandas_df['weight']). The weights reach the
right edges only if igraph edge k is row k of the table. This script copies the thesis functions
verbatim, builds the graph from a small edge table, and compares the orders. It also checks that
the numpy emulation used in experiments/scale_test (nx_order) gives the same order.
Run: python -I edge_order_check.py > run_log.txt
"""
import random
import networkx as nx
import igraph as ig
import numpy as np
import pandas as pd


# --- copied from hm31_graphs/step_10_cluster/construct_base_graph.py ---
def construct_graph(pandas_df):
    G = nx.from_pandas_edgelist(pandas_df, 'first', 'second')
    G, mapping_df = relabel_networkx_nodes(G)
    H = ig.Graph.from_networkx(G)
    return H, G, mapping_df


def relabel_networkx_nodes(G_networkx):
    nodes = G_networkx.nodes
    mapping_list = []
    mapping_dict = {}
    for i, node_id in enumerate(nodes):
        mapping_list.append({'original_id': node_id, 'squashed_id': i})
        mapping_dict[node_id] = i
    G_networkx_relabled = nx.relabel_nodes(G_networkx, mapping_dict)
    mapping_df = pd.DataFrame(mapping_list)
    return G_networkx_relabled, mapping_df
# --- end of copy ---


def nx_order(first, second):
    """Row index of each igraph edge under the thesis construction, computed with numpy."""
    seq = np.empty(2 * first.size, dtype=first.dtype)
    seq[0::2], seq[1::2] = first, second
    uniq, idx = np.unique(seq, return_index=True)
    rank_of = dict(zip(uniq.tolist(), np.argsort(np.argsort(idx)).tolist()))  # small-graph version
    r1 = np.array([rank_of[x] for x in first.tolist()])
    r2 = np.array([rank_of[x] for x in second.tolist()])
    return np.lexsort((np.arange(first.size), np.minimum(r1, r2)))


random.seed(20261024)
for n, m in [(50, 120), (2000, 10000)]:
    pairs = set()
    while len(pairs) < m:
        a, b = random.randrange(n), random.randrange(n)
        if a != b and (a, b) not in pairs and (b, a) not in pairs:
            pairs.add((a, b))
    rows = list(pairs)
    random.shuffle(rows)
    df = pd.DataFrame(rows, columns=['first', 'second'])
    H, G, mapping = construct_graph(df)
    orig = mapping['original_id'].to_numpy()
    # original endpoints of each igraph edge, as an unordered pair
    ig_pairs = [frozenset((orig[e.source], orig[e.target])) for e in H.es]
    row_pairs = [frozenset(r) for r in rows]
    same = sum(a == b for a, b in zip(ig_pairs, row_pairs))
    pos = {p: i for i, p in enumerate(row_pairs)}
    perm = np.array([pos[p] for p in ig_pairs])
    emu = nx_order(df['first'].to_numpy(), df['second'].to_numpy())
    print(f"n={n} m={m}: igraph edge k == table row k for {same}/{m} edges; "
          f"numpy emulation matches thesis order: {np.array_equal(perm, emu)}")


# Same check for the full-scale function used in the clustering runs (scale_test/run_cpm.py:prepare_nx_order)
import os, sys, tempfile
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scale_test"))
import run_cpm
with tempfile.TemporaryDirectory() as wd:
    ids, inv = np.unique(np.concatenate([df['first'].to_numpy(), df['second'].to_numpy()]), return_inverse=True)
    np.save(os.path.join(wd, "u.npy"), inv[:m].astype(np.int32))
    np.save(os.path.join(wd, "v.npy"), inv[m:].astype(np.int32))
    run_cpm.prepare_nx_order({"work_dir": wd}, wd)
    full = np.load(os.path.join(wd, "nx_order.npy"))
    w = np.random.default_rng(0).random(m)
    thesis_w = np.empty(m); thesis_w[perm] = w  # edge row perm[k] got weight row k
    print(f"run_cpm.prepare_nx_order matches thesis order: {np.array_equal(full, perm)}; "
          f"misalign() reproduces the thesis weight assignment: "
          f"{np.array_equal(run_cpm.misalign(w, 'nx_order', {'work_dir': wd}, wd), thesis_w)}")
