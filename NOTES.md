# Notes

## Part 1: Incidents

| Incident | Root cause | Reproducing test | Fix |
|----------|------------|------------------|-----|
| INC-1 | `load` / check / `store` on `running` was not an atomic reservation. Concurrent callers could both receive permits while only one increment was recorded; later releases could underflow the counter. | `inc_1_never_admits_more_than_capacity` | CAS loop using `compare_exchange` to reserve a slot only if the observed count is unchanged. |
| INC-2 | `max_running - running` underflowed when the limit was reduced below the number of active queries, causing a debug panic or a misleading available-capacity value. | `inc_2_capacity_cut_does_not_block_other_clusters` | Use `saturating_sub` so availability is zero when a cluster is already over its new limit. Existing queries continue running. |
| INC-3 | The selected cluster could become full between selection and reservation, and `try_acquire` returned `Busy` without trying other clusters. | `inc_3_does_not_return_busy_while_capacity_is_free` | If reservation on the preferred cluster fails, attempt the remaining clusters before returning `Busy`. |
| INC-4 | Dropping a `Permit` without calling `release` leaked the admission slot, e.g. after cancellation or early return. | `inc_4_dropped_permit_releases_slot`, `inc_4_explicit_release_does_not_double_decrement` | Implement `Drop` for `Permit`; explicit `release(self)` delegates to `drop(self)`. Keep an `Arc<ClusterState>` in the permit to release the originally reserved state. |

## Part 2: Performance

Machine (CPU and core count): **MacBook Pro, Apple M1 Pro, 10 CPU cores (8 Performance + 2 Efficiency), 10 physical / 10 logical cores, 16 GB RAM.** Rust/Cargo 1.87.0.

| Bench | Before | After |
|-------|--------|-------|
| acquire_release/1 | 1.7824 M operations/s | 12.737 M operations/s |
| acquire_release/4 | 2.4950 M operations/s | 3.4140 M operations/s |
| acquire_release/16 | 1.9284 M operations/s | 3.5499 M operations/s |

Numbers are Criterion throughput point estimates from the initial benchmark and the final `lakeops-bench-part3-opt1.txt` run on the same machine. The improvement is approximately **7.15x** at 1 thread, **1.37x** at 4 threads, and **1.84x** at 16 threads. The 16-thread result is **below the requested 3x target**.

Changes, and why each one helps (or didn't):

- `Permit` stores `Arc<ClusterState>` so release does not re-look-up the cluster under the group lock. This also ties release to the state originally reserved.
- Per-cluster `Arc<AtomicU64>` admission counters avoid locking one global admissions mutex on each successful acquire. This change alone produced no measurable 16-thread throughput improvement, but preserves counters across config reloads.
- `try_acquire` iterates over borrowed cluster references rather than cloning a vector and allocating a `Candidate` list on every attempt; this was the largest observed improvement.
- `parking_lot::RwLock` replaces the standard group lock, improving the contended 16-thread workload. This is the one added dependency; it provides a smaller, non-poisoning lock API, though it was not uniformly faster for every thread count.
- An `AtomicUsize` rotates the starting position for selection, reducing preference for the same cluster on ties, at the cost of another atomic operation and modulo calculations.
- Part 3 uses a read lock for the waiter map on release rather than an exclusive mutex. This recovered performance at 16 threads, although the 4-thread measurement regressed in that comparison.

Selection policy, and why: choose a cluster with the most currently available slots, rotating the scan start to spread tied choices. If the chosen cluster fills concurrently, try other clusters before returning `Busy`. This keeps selection roughly least-loaded without a global exclusive acquisition lock. The choice is approximate because capacity can change between observation and CAS.

## Part 3: Waiting for a slot

`acquire_timeout` tries an immediate admission first. On `Busy`, it registers with a per-group Tokio `Notify`, increments the queued counter, and waits until a release notification or its deadline. It enables the notification future before rechecking capacity to avoid a check-then-sleep lost-wakeup window, and retries admission after waking. `queued(group)` reads the counter. A `QueueGuard` decrements it via `Drop`, including on timeout or future cancellation. Release decrements the cluster count and calls `notify_one`. The six provided waiting tests and an additional zero-timeout test pass.

## Part 4: Global admission across 6 pods

Local counters cannot enforce a shared cap: six pods can each admit 40 queries to a cluster with a global limit of 40. I would use PostgreSQL as a shared source of truth for reservations. Each admission would run a short transaction: lock the cluster's capacity row, check the active reservation count against the configured limit, insert a uniquely identified reservation (idempotent on retry), and commit before dispatching the query. Completion would release the reservation idempotently. This serializes competing reservations for a cluster across all six pods.

The hot-path cost is a network round trip, a database transaction, and possible row-lock contention on busy clusters. On pod death, heartbeat/lease information would identify suspect reservations, but recovery must verify the engine query has stopped (or cancel it) before reusing capacity: an expired lease alone is not proof the query ended. During a network partition or database outage I would **fail closed** for new admissions to preserve the global cap. I would deliberately not build custom consensus, a distributed lock service, or an elaborate scheduler for this exercise; I would first assess whether the simpler database-based approach meets latency and availability needs.

## Bonus: INC-5 and INC-6

Not implemented. `reconcile` can overwrite concurrent increments/decrements with a stale engine count (INC-5). `reload` recreates cluster state and resets visible running counts for active queries; preserving an admissions metric is not equivalent to preserving the live reservation state (INC-6).

## Before shipping, I would still want

- Repeat performance measurements under controlled conditions. The required 3x throughput improvement is not met.
- Stress-test concurrent capacity changes, reloads with live permits, and reconciliation with ongoing admissions/releases; implement the bonus fixes if required for production.
- Add deterministic cancellation/wakeup interleaving tests, including multiple waiters and notification/cancellation races.
- Run formatting, Clippy, debug and release test suites, and review the final Git diff before submitting.
