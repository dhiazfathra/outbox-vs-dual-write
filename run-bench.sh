#!/usr/bin/env bash
# Runs every arm x repetition under the shared benchmark lock.
# Rep 1 of each arm is a warm-up and is discarded when reporting.
set -euo pipefail
cd "$(dirname "$0")"

N=${N:-50000}
REPS=${REPS:-4}

LOCK=/tmp/expbrief/benchlock.sh
ME=outbox-vs-dual-write

# Build and start everything BEFORE taking the lock: the lock covers load only.
docker compose up -d
echo "waiting for containers to be healthy ..."
for _ in $(seq 1 60); do
	ok=$(docker inspect -f '{{.State.Health.Status}}' ovd-postgres ovd-redpanda 2>/dev/null | grep -c healthy || true)
	[ "$ok" = "2" ] && break
	sleep 2
done
docker inspect -f '{{.Name}} {{.State.Health.Status}}' ovd-postgres ovd-redpanda

go build -o bin/bench .
mkdir -p results

if [ -x "$LOCK" ]; then
	until "$LOCK" acquire "$ME"; do echo "re-issuing lock acquire ..."; done
	release_lock() { "$LOCK" release "$ME" || true; }
	# Only armed after a successful acquire, so it can never release someone
	# else's lock; cleared explicitly once the matrix finishes.
	trap release_lock EXIT
else
	echo "WARNING: $LOCK missing; running without the shared benchmark lock" >&2
	release_lock() { :; }
fi

for arm in dualwrite outbox; do
	for mode in control inject; do
		for rep in $(seq 1 "$REPS"); do
			flag=$([ "$mode" = inject ] && echo -inject=true || echo -inject=false)
			echo "=== $arm $mode rep$rep ==="
			./bin/bench -arm="$arm" "$flag" -n="$N" -rep="$rep" \
				2>&1 | tee "results/${arm}-${mode}-rep${rep}.log"
			# make sure the broker is up again before the next run
			docker start ovd-redpanda >/dev/null 2>&1 || true
			sleep 8
		done
	done
done

release_lock
trap - EXIT

go run ./cmd/aggregate >results/SUMMARY.md
cat results/SUMMARY.md
