# Scale-identity check (new, not in the thesis)

Numerical check of the identity used in Section 4.4 of the paper *Similarity-Weighted Leiden-CPM Clustering of a
7.4-Million-Node Biomedical Citation Graph*: for CPM quality H(P; w, γ) = Σ_c (e_c(w) − γ C(n_c, 2)) and any α > 0,
H(P; αw, γ) = α H(P; w, γ/α), so scaling all weights by α gives the same optimal partitions as resolution γ/α.

- Part 1: 2,400 random partitions of a random 60-node weighted graph (max deviation 9.1e-13).
- Part 2: all 4,140 partitions of an 8-node graph; the best partition is the same for every tested α and γ.

Synthetic graphs only; no project data. Run: `python3 -I scale_equivalence.py > run_log.txt` (numpy; seed in
`config.json`). `code_checksums.sha256` covers the script and config.
