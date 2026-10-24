"""Checks that igraph community_leiden is deterministic given random.seed (same seed -> same partition).
Small random graph, so it runs in seconds. Run: python -I determinism_check.py > determinism_check_log.txt"""
import random, igraph as ig
def run(seed, w):
    random.seed(1234); g = ig.Graph.Erdos_Renyi(n=3000, m=15000)
    ws = [random.random() for _ in range(g.ecount())] if w else None
    random.seed(seed)
    return g.community_leiden(objective_function="CPM", weights=ws, resolution=0.05, n_iterations=2).membership
for w in (False, True):
    a, b, c = run(4311, w), run(4311, w), run(1, w)
    print("weighted" if w else "unweighted", "same seed identical:", a == b, "| different seed differs:", a != c)
