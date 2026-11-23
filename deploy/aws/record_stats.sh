#!/usr/bin/env bash
# ==============================================================================
# record_stats.sh — Aegis-FDS 48h Soak Test Docker Resource Monitor
#
# Records `docker stats --no-stream` metrics every 5 minutes (300 seconds)
# with both UTC and KST timestamps.
#
# Output:
#   ~/soak_stats.csv
#
# Background Execution:
#   nohup bash deploy/aws/record_stats.sh > ~/soak_stats_runner.log 2>&1 &
# ==============================================================================

set -euo pipefail

OUTPUT_FILE="${HOME}/soak_stats.csv"
INTERVAL_SECONDS=300

# Write CSV header if file does not exist or is empty
if [ ! -s "$OUTPUT_FILE" ]; then
    echo "timestamp_utc,timestamp_kst,container_name,cpu_perc,mem_usage,mem_limit,mem_perc,net_in,net_out,block_in,block_out,pids" > "$OUTPUT_FILE"
fi

echo "=================================================================="
echo " Aegis-FDS Soak Test Resource Monitor Started"
echo " Target Output : $OUTPUT_FILE"
echo " Poll Interval : ${INTERVAL_SECONDS}s (5 minutes)"
echo " Press [Ctrl+C] to stop."
echo "=================================================================="

trap 'echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] Monitor stopped."; exit 0' SIGINT SIGTERM

while true; do
    TS_UTC=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    TS_KST=$(TZ="Asia/Seoul" date +"%Y-%m-%d %H:%M:%S" 2>/dev/null || date +"%Y-%m-%d %H:%M:%S")

    # Fetch stats formatted as tab/pipe-separated lines
    # raw format: Container | Name | CPUPerc | MemUsage | MemPerc | NetIO | BlockIO | PIDs
    STATS=$(docker stats --no-stream --format "{{.Name}},{{.CPUPerc}},{{.MemUsage}},{{.MemPerc}},{{.NetIO}},{{.BlockIO}},{{.PIDs}}" 2>/dev/null || true)

    if [ -n "$STATS" ]; then
        while IFS= read -r line; do
            if [ -n "$line" ]; then
                # line format: Name,CPUPerc,MemUsage / MemLimit,MemPerc,NetIn / NetOut,BlockIn / BlockOut,PIDs
                # Clean up units or spaces if needed
                IFS=',' read -r c_name c_cpu c_mem c_memp c_net c_blk c_pids <<< "$line"

                # Parse MemUsage / MemLimit
                mem_u=$(echo "$c_mem" | awk -F '/' '{print $1}' | tr -d ' ')
                mem_l=$(echo "$c_mem" | awk -F '/' '{print $2}' | tr -d ' ')

                # Parse NetIO (in / out)
                net_i=$(echo "$c_net" | awk -F '/' '{print $1}' | tr -d ' ')
                net_o=$(echo "$c_net" | awk -F '/' '{print $2}' | tr -d ' ')

                # Parse BlockIO (in / out)
                blk_i=$(echo "$c_blk" | awk -F '/' '{print $1}' | tr -d ' ')
                blk_o=$(echo "$c_blk" | awk -F '/' '{print $2}' | tr -d ' ')

                echo "${TS_UTC},${TS_KST},${c_name},${c_cpu},${mem_u},${mem_l},${c_memp},${net_i},${net_o},${blk_i},${blk_o},${c_pids}" >> "$OUTPUT_FILE"
            fi
        done <<< "$STATS"
    fi

    sleep "$INTERVAL_SECONDS"
done
