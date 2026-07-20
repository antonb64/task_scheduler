#!/bin/sh
set -eu

project_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$project_dir"
runtime_dir="$project_dir/.local"
mkdir -p "$runtime_dir"

export SCHEDULER_MASTER_KEY="AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
export SCHEDULER_ADMIN_TOKEN="dev-admin-token"
export SCHEDULER_ARTIFACT_ROOTS="$project_dir/examples"
export SCHEDULER_DATABASE_URL="sqlite://$runtime_dir/coordinator.db"
export SCHEDULER_LOCK_PATH="$runtime_dir/coordinator.lock"

cargo build --workspace

"$project_dir/target/debug/coordinator" &
coordinator_pid=$!

cleanup() {
  kill "$coordinator_pid" 2>/dev/null || true
  if [ -n "${agent_pid:-}" ]; then kill "$agent_pid" 2>/dev/null || true; fi
}
trap cleanup EXIT INT TERM

sleep 1
"$project_dir/target/debug/agent" \
  --agent-id local-node \
  --database-url "sqlite://$runtime_dir/agent.db" \
  --executor-path "$project_dir/target/debug/task-executor" &
agent_pid=$!

echo "Coordinator UI: http://127.0.0.1:8080 (token: dev-admin-token)"
echo "Agent UI proxy: http://127.0.0.1:8081 (same token)"
echo "Press Ctrl-C to stop."
wait "$coordinator_pid"
