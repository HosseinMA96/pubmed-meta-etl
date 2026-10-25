#!/bin/bash
# Batch 7c helper: while a run_cpm.py run is active, kill Microsoft Edge if MemAvailable drops below
# THRESH_MB (user's permission, 2026-10-24). Logs every check above a warning level and every kill.
cd "$(dirname "$0")"
THRESH_MB=${THRESH_MB:-1500}; WARN_MB=2500; LOG=watchdog_log.txt
echo "$(date +%H:%M:%S) watchdog start (kill Edge below ${THRESH_MB} MB available)" >> $LOG
while pgrep -f "run_list.sh" > /dev/null; do
  avail=$(( $(awk '/MemAvailable/ {print $2}' /proc/meminfo) / 1024 ))
  if [ "$avail" -lt "$THRESH_MB" ] && pgrep -x msedge > /dev/null; then
    pkill -x msedge; echo "$(date +%H:%M:%S) MemAvailable ${avail} MB: killed Edge" >> $LOG
  elif [ "$avail" -lt "$WARN_MB" ]; then
    echo "$(date +%H:%M:%S) MemAvailable ${avail} MB (warning)" >> $LOG
  fi
  sleep 5
done
echo "$(date +%H:%M:%S) watchdog stop (batch finished)" >> $LOG
