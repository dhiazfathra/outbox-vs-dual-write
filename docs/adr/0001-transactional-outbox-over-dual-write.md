# ADR 0001: Use a transactional outbox, not a dual write, for order events

## Status

Accepted

## Date

2026-08-24

## Context

Order writes have to land in PostgreSQL and also reach a Kafka-compatible broker
(Redpanda). The two obvious shapes are:

1. **Dual write** — commit the row, then publish the event synchronously in the
   request path. One moving part, no background job, and the publish latency is
   visible to the caller.
2. **Transactional outbox** — write the row and an `outbox` row in one
   transaction, and let a relay poller publish unsent rows and mark them sent.
   The write path never talks to the broker; a background loop does.

The question we needed answered was not "which is more correct in theory" — the
outbox obviously is — but **how much data a dual write actually loses on this
hardware for a realistic broker outage**, so we could weigh it against the cost
of running a relay.

We measured it. `docker kill` on the broker at 40% of a fixed-seed 50,000-order
workload, a fixed pause, `docker start`, identical injection timing for both
strategies, plus a no-injection control arm each. Ground truth is the `orders`
table; the consumer sink is reconciled against it as a set difference. Four
repetitions per arm, the first discarded as warm-up. Raw output is in
`results/`.

## Decision

Use the transactional outbox for anything where a missing event has business
consequences (order placed, payment captured, inventory moved).

The measured outcome is unambiguous: under an identical broker kill, dual write
lost a four-figure number of events and the outbox lost zero, with zero
duplicates in both. The write-path latency of the outbox is also _lower_ than
dual write, because the write path no longer contains a network round trip to
the broker — the cost moves into end-to-end lag instead, where it is a queue you
can watch, not data you cannot get back.

See `results/SUMMARY.md` for the numbers and `WRITEUP.mdx` for the analysis.

## Alternatives Considered

**Dual write with synchronous publish (measured, rejected).** It loses events,
and it loses them exactly when you can least afford it: the publish failures all
land inside the outage window, so they are correlated, not spread thin. It also
puts broker latency and broker availability directly into the request path, so
the p99 of the write path degrades with the broker. It is simpler, and that is
its only advantage.

**Dual write with an in-process retry queue (rejected without measuring).** This
is an outbox with the durable part removed. It survives a broker restart but not
a process restart, an OOM kill, or a deploy — and those are more frequent than
broker restarts. If you are going to build the queue, put it in the database
where the transaction already is.

**CDC via Debezium + Kafka Connect (attempted, dropped).** This was the third
arm in the plan. It did not fit: the machine had under 10 GB of free disk at the
start and the Docker VM ran out of space mid-experiment and hard-stopped the
daemon; Debezium plus Kafka Connect is roughly another gigabyte of images on top
of Postgres and Redpanda. We dropped it rather than run it on a disk that had
already failed once, because a measurement taken on a storage-starved VM is not
a measurement. It is not reported, estimated, or extrapolated anywhere. CDC is
the right thing to evaluate next, on a machine with room.

**Kafka transactions / exactly-once semantics (out of scope).** Exactly-once
between the broker and a consumer does not help here: the gap being measured is
between PostgreSQL and the broker, and no amount of broker-side EOS closes it.

## Consequences

**Good.** No event loss across a broker restart, from a mechanism that is a
table and a loop, not a framework. Lower write-path latency, because the request
no longer waits on the broker. The outbox table is also an audit log of what was
supposed to be published, which is worth something on its own during an
incident.

**Bad, and you are signing up for these.**

- _Lag replaces loss._ During the outage the outbox grows and end-to-end lag
  grows with it. Anything downstream that assumes near-real-time delivery will
  be wrong for the length of the outage plus the drain. You now need a lag
  alert; you did not before.
- _At-least-once, not exactly-once._ The relay publishes then marks sent. A
  relay crash between those two steps replays the batch. We measured zero
  duplicates because the relay did not crash — the design still requires
  consumers to be idempotent, and any write-up that reads "0 dupes" as
  "exactly-once" is misreading it.
- _A second thing to operate._ The relay is a process that can fall behind, get
  stuck on a poison row, or be accidentally scaled to two instances and start
  double-publishing. Single-relay assumptions need a lock or a leader election
  once it stops being one process.
- _Write amplification._ Every order write is now two inserts plus a later
  update in the same database. The outbox table needs a retention job or it
  becomes the largest table you own.
- _Ordering is only as good as the poller._ We used one partition and
  `ORDER BY seq`. Partitioned topics with concurrent relay workers reorder
  events, and nothing in this design prevents that.
