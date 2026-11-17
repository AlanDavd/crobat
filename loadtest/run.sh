#!/usr/bin/env bash
# Peak load test: k6 creates TASKS one-shot tasks through task-api at RATE/s,
# all due at the same second, then we measure how late each job started.
#
#   loadtest/run.sh                       # 30k tasks at 1000/s, 4 tenants
#   TASKS=5000 RATE=500 loadtest/run.sh
#
# Needs the stack running (docker compose up -d) and .env.
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; source .env; set +a

TASKS=${TASKS:-30000}
RATE=${RATE:-1000}
TENANTS=${TENANTS:-4}
LEAD=${LEAD:-45}                  # seconds between the last create and the peak
K6_IMAGE=grafana/k6:1.8.0
NETWORK=$(docker network ls --format '{{.Name}}' | grep -m1 '_crobat$')
RUN="lt$(date +%s)"
sql() { docker compose exec -T mysql mysql -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" crobat "$@" 2>/dev/null; }

go build -o bin/crobat ./cmd/crobat
TOKENS=$(for t in $(seq 0 $((TENANTS - 1))); do bin/crobat token "$RUN-t$t"; done | paste -sd, -)

create_s=$(( (TASKS + RATE - 1) / RATE ))
AT=$(date -u -r $(( $(date +%s) + create_s + LEAD )) +%Y-%m-%dT%H:%M:%SZ 2>/dev/null \
  || date -u -d "@$(( $(date +%s) + create_s + LEAD ))" +%Y-%m-%dT%H:%M:%SZ)

echo "run=$RUN tasks=$TASKS rate=$RATE/s tenants=$TENANTS peak_at=$AT"
docker compose stop archiver >/dev/null 2>&1   # keep finished jobs in MySQL for the report
trap 'docker compose start archiver >/dev/null 2>&1' EXIT

docker run --rm -i --network "$NETWORK" \
  -e RUN="$RUN" -e AT="$AT" -e TASKS="$TASKS" -e RATE="$RATE" -e TOKENS="$TOKENS" \
  "$K6_IMAGE" run --quiet - < loadtest/create-tasks.js || echo "k6 thresholds failed (see above)"

echo "waiting for the peak at $AT and for every job to finish..."
for _ in $(seq 1 600); do
  left=$(sql -N -e "SELECT
      (SELECT COUNT(*) FROM tasks WHERE tenant_id LIKE '$RUN%' AND next_run_at IS NOT NULL) +
      (SELECT COUNT(*) FROM jobs  WHERE tenant_id LIKE '$RUN%' AND status IN ('pending','running'))")
  [ "$left" = "0" ] && break
  sleep 1
done

{ echo "SET @run = '$RUN';"; cat loadtest/lag.sql; } | sql -t

# Delete this run's rows now, so InnoDB purges them while idle instead of
# during the next run. KEEP=1 keeps them for inspection.
if [ "${KEEP:-0}" != 1 ]; then
  sql -e "DELETE FROM jobs WHERE tenant_id LIKE 'lt%'; DELETE FROM tasks WHERE tenant_id LIKE 'lt%';"
fi
