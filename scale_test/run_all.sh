#!/bin/bash
# Runs the scale-test clusterings one at a time, each under a memory cap so the
# machine cannot run out of memory. Skips runs whose result JSON already exists.
# v2: logs the real exit status, allows a per-run cap ("RUN:CAP"), and adds the
# alignment runs. v1 (run_all_v1.sh) produced the first 23 runs.
cd "$(dirname "$0")"
PY=~/anaconda3/envs/leiden/bin/python
[ -f work/nx_order.npy ] || systemd-run --user --scope -q -p MemoryMax=8G -p MemorySwapMax=0 $PY -I run_cpm.py config.json prep_nx
ORDER="intuitive_raw_nxorder_s4311 topo_equal_map110_nxorder_s4311 intuitive_raw_shuffle_s4311
topo_equal_map110_shuffle_s4311 baseline_s4311_repeat cocF_0.875_raw_s4311:12G topo_equal_raw_nxorder_s4311:12G"
mkdir -p logs
for item in $ORDER; do
  r=${item%%:*}; CAP=${item#*:}; [ "$CAP" = "$item" ] && CAP=
  [ -f "runs/$r.json" ] && continue
  [ -z "$CAP" ] && case $r in baseline_*) CAP=10G ;; *) CAP=11G ;; esac
  echo "$(date +%H:%M:%S) start $r (cap $CAP)"
  systemd-run --user --scope -q -p MemoryMax=$CAP -p MemorySwapMax=0 \
    $PY -I run_cpm.py config.json "$r" > "logs/$r.log" 2>&1
  st=$?
  if [ -f "runs/$r.json" ]; then echo "$(date +%H:%M:%S) end $r ok"; else echo "$(date +%H:%M:%S) end $r FAILED (status $st, no JSON; see logs/$r.log)"; fi
done
echo "all done"
