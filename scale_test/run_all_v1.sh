#!/bin/bash
# Runs the scale-test clusterings one at a time, each under a memory cap so the
# machine cannot run out of memory. Skips runs whose result JSON already exists.
cd "$(dirname "$0")"
PY=~/anaconda3/envs/leiden/bin/python
ORDER="intuitive_raw_s4311 intuitive_meannorm_s4311 topo_equal_raw_s4311 topo_equal_meannorm_s4311
topo_equal_map110_s4311 bibJ_0.875_raw_s4311 bibJ_0.875_meannorm_s4311 cocF_0.875_raw_s4311
cocF_0.875_meannorm_s4311 baseline_s1 baseline_s2 intuitive_meannorm_s1 topo_equal_meannorm_s1
topo_equal_map110_s1 baseline_s3 baseline_s4 intuitive_meannorm_s2 topo_equal_meannorm_s2
topo_equal_map110_s2 bibJ_0.875_meannorm_s1 bibJ_0.875_meannorm_s2 cocF_0.875_meannorm_s1
cocF_0.875_meannorm_s2"
mkdir -p logs
for r in $ORDER; do
  [ -f "runs/$r.json" ] && continue
  case $r in baseline_*) CAP=10G ;; *) CAP=11G ;; esac
  echo "$(date +%H:%M:%S) start $r (cap $CAP)"
  systemd-run --user --scope -q -p MemoryMax=$CAP -p MemorySwapMax=0 \
    $PY -I run_cpm.py config.json "$r" > "logs/$r.log" 2>&1
  echo "$(date +%H:%M:%S) end $r exit $?"
done
echo "all done"
