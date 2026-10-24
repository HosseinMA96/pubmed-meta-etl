# Edge-order check (new, not in the thesis)

Question: in the thesis weighted runs, did each weight reach its own edge?

Thesis code path:
- Graph: `hm31_graphs/step_10_cluster/construct_base_graph.py:construct_graph` builds the igraph graph from the edge
  table `files/edges/` via `nx.from_pandas_edgelist` → `relabel_networkx_nodes` → `ig.Graph.from_networkx`.
- Weights: `weighting_clustering_pipeline.py:convert_pyspark_features_to_pandas` writes the feature table ordered by
  `id` (`files/raw_features.h5`); `transformation.py` computes `weight` row by row in that order;
  `weighting_clustering_pipeline.py:leiden` passes `weights=pandas_df['weight']` to `la.find_partition`, i.e. by position.
- Nothing re-aligns weights to the igraph edge order (no `es[...]` attribute, join, or `get_eid`).

Result (`edge_order_check.py`, log `run_log.txt`, functions copied verbatim from the thesis, networkx 2.8.8 as the
thesis's `nx.read_gpickle` requires networkx < 3): igraph edge k equals table row k for only 2/120 and 2/10,000 edges on
random tables. So weights passed by position land on other edges unless the table was already in networkx iteration
order. On the full CENM row order of `all_edges_features.parquet` (`scale_test/work/nx_order.npy`), 3 of 57,512,086
edges keep their row position.

Not known: whether `files/edges/` and the `id` order of the feature table equal the row order of our local parquet
(both tables come from the same Spark pipeline; `id` was added with monotonically_increasing_id). The clustering
tests in `scale_test/` therefore use two variants: `nx_order` (assumes our parquet row order is the thesis order) and
`shuffle` (a random permutation, assumption-free).
