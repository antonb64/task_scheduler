#!/bin/sh

# Run the deterministic state-machine, persistence, and crash/replay suites.
# Every generated range is derived from the profile and zero-based shard index,
# so CI failures can be reproduced with the variables printed below.
set -eu

profile=${SCHEDULER_SIM_PROFILE:-fast}
shard_index=${SCHEDULER_SIM_SHARD_INDEX:-0}
shard_count=${SCHEDULER_SIM_SHARD_COUNT:-1}
base_seed=${SCHEDULER_SIM_BASE_SEED:-1000000}

require_uint() {
    name=$1
    value=$2
    case "$value" in
        ''|*[!0-9]*)
            echo "$name must be an unsigned decimal integer, got: $value" >&2
            exit 2
            ;;
    esac
}

require_positive() {
    name=$1
    value=$2
    require_uint "$name" "$value"
    if [ "$value" -eq 0 ]; then
        echo "$name must be greater than zero" >&2
        exit 2
    fi
}

require_uint SCHEDULER_SIM_SHARD_INDEX "$shard_index"
require_positive SCHEDULER_SIM_SHARD_COUNT "$shard_count"
require_uint SCHEDULER_SIM_BASE_SEED "$base_seed"
if [ "$shard_index" -ge "$shard_count" ]; then
    echo "SCHEDULER_SIM_SHARD_INDEX must be smaller than SCHEDULER_SIM_SHARD_COUNT" >&2
    exit 2
fi

case "$profile" in
    fast)
        default_core_seeds=24
        default_core_steps=400
        default_cron_seeds=64
        default_store_seeds=8
        default_store_steps=240
        default_protocol_cases=128
        default_health_seeds=512
        default_health_steps=240
        ;;
    soak)
        default_core_seeds=256
        default_core_steps=2000
        default_cron_seeds=512
        default_store_seeds=32
        default_store_steps=1000
        default_protocol_cases=512
        default_health_seeds=4096
        default_health_steps=2000
        ;;
    *)
        echo "SCHEDULER_SIM_PROFILE must be 'fast' or 'soak', got: $profile" >&2
        exit 2
        ;;
esac

SCHEDULER_SIM_SEEDS=${SCHEDULER_SIM_SEEDS:-$default_core_seeds}
SCHEDULER_SIM_STEPS=${SCHEDULER_SIM_STEPS:-$default_core_steps}
SCHEDULER_CRON_SIM_SEEDS=${SCHEDULER_CRON_SIM_SEEDS:-$default_cron_seeds}
SCHEDULER_STORE_SIM_SEEDS=${SCHEDULER_STORE_SIM_SEEDS:-$default_store_seeds}
SCHEDULER_STORE_SIM_STEPS=${SCHEDULER_STORE_SIM_STEPS:-$default_store_steps}
SCHEDULER_SIM_CASES=${SCHEDULER_SIM_CASES:-$default_protocol_cases}
SCHEDULER_HEALTH_SIM_SEEDS=${SCHEDULER_HEALTH_SIM_SEEDS:-$default_health_seeds}
SCHEDULER_HEALTH_SIM_STEPS=${SCHEDULER_HEALTH_SIM_STEPS:-$default_health_steps}

require_positive SCHEDULER_SIM_SEEDS "$SCHEDULER_SIM_SEEDS"
require_positive SCHEDULER_SIM_STEPS "$SCHEDULER_SIM_STEPS"
require_positive SCHEDULER_CRON_SIM_SEEDS "$SCHEDULER_CRON_SIM_SEEDS"
require_positive SCHEDULER_STORE_SIM_SEEDS "$SCHEDULER_STORE_SIM_SEEDS"
require_positive SCHEDULER_STORE_SIM_STEPS "$SCHEDULER_STORE_SIM_STEPS"
require_positive SCHEDULER_SIM_CASES "$SCHEDULER_SIM_CASES"
require_positive SCHEDULER_HEALTH_SIM_SEEDS "$SCHEDULER_HEALTH_SIM_SEEDS"
require_positive SCHEDULER_HEALTH_SIM_STEPS "$SCHEDULER_HEALTH_SIM_STEPS"

SCHEDULER_SIM_SEED_START=${SCHEDULER_SIM_SEED_START:-$((base_seed + shard_index * SCHEDULER_SIM_SEEDS))}
SCHEDULER_CRON_SIM_SEED_START=${SCHEDULER_CRON_SIM_SEED_START:-$((base_seed + shard_index * SCHEDULER_CRON_SIM_SEEDS))}
SCHEDULER_STORE_SIM_SEED_START=${SCHEDULER_STORE_SIM_SEED_START:-$((base_seed + shard_index * SCHEDULER_STORE_SIM_SEEDS))}
SCHEDULER_SIM_SEED=${SCHEDULER_SIM_SEED:-$((base_seed + shard_index))}
SCHEDULER_HEALTH_SIM_SEED_START=${SCHEDULER_HEALTH_SIM_SEED_START:-$((base_seed + shard_index * SCHEDULER_HEALTH_SIM_SEEDS))}

require_uint SCHEDULER_SIM_SEED_START "$SCHEDULER_SIM_SEED_START"
require_uint SCHEDULER_CRON_SIM_SEED_START "$SCHEDULER_CRON_SIM_SEED_START"
require_uint SCHEDULER_STORE_SIM_SEED_START "$SCHEDULER_STORE_SIM_SEED_START"
require_uint SCHEDULER_SIM_SEED "$SCHEDULER_SIM_SEED"
require_uint SCHEDULER_HEALTH_SIM_SEED_START "$SCHEDULER_HEALTH_SIM_SEED_START"

export SCHEDULER_SIM_SEED_START SCHEDULER_SIM_SEEDS SCHEDULER_SIM_STEPS
export SCHEDULER_CRON_SIM_SEED_START SCHEDULER_CRON_SIM_SEEDS
export SCHEDULER_STORE_SIM_SEED_START SCHEDULER_STORE_SIM_SEEDS SCHEDULER_STORE_SIM_STEPS
export SCHEDULER_SIM_SEED SCHEDULER_SIM_CASES
export SCHEDULER_HEALTH_SIM_SEED_START SCHEDULER_HEALTH_SIM_SEEDS SCHEDULER_HEALTH_SIM_STEPS

echo "simulation profile=$profile shard=$shard_index/$shard_count"
echo "core seed_start=$SCHEDULER_SIM_SEED_START seeds=$SCHEDULER_SIM_SEEDS steps=$SCHEDULER_SIM_STEPS"
echo "cron seed_start=$SCHEDULER_CRON_SIM_SEED_START seeds=$SCHEDULER_CRON_SIM_SEEDS"
echo "store seed_start=$SCHEDULER_STORE_SIM_SEED_START seeds=$SCHEDULER_STORE_SIM_SEEDS steps=$SCHEDULER_STORE_SIM_STEPS"
echo "protocol seed=$SCHEDULER_SIM_SEED cases=$SCHEDULER_SIM_CASES"
echo "health seed_start=$SCHEDULER_HEALTH_SIM_SEED_START seeds=$SCHEDULER_HEALTH_SIM_SEEDS steps=$SCHEDULER_HEALTH_SIM_STEPS"

cargo test --locked -p scheduler-core --test model_simulation -- --nocapture
cargo test --locked -p scheduler-core --test health_simulation -- --nocapture
cargo test --locked -p scheduler-store --test deterministic_simulation -- --nocapture
cargo test --locked -p coordinator --test protocol_simulation -- --nocapture
