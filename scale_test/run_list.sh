#!/bin/bash
# Like run_all.sh, but runs the names given as arguments ("RUN" or "RUN:CAP"), in order.
# Usage: ./run_list.sh baseline_g0.1_s4311 topo_equal_raw_nxorder_s4311:12G >> batch_log.txt 2>&1
cd "$(dirname "$0")"
PY=~/anaconda3/envs/leiden/bin/python
mkdir -p logs
for item in "$@"; do
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
