# Results (medians over repetitions, rep 1 discarded as warm-up)

| arm               | reps | written | lost | dupes | write p50 ms | write p95 ms | write p99 ms | lag p50 ms | lag p99 ms | lag max ms | recovery s | writes/s |
| ----------------- | ---- | ------- | ---- | ----- | ------------ | ------------ | ------------ | ---------- | ---------- | ---------- | ---------- | -------- |
| dualwrite/control | 3    | 50000   | 0    | 0     | 6.65         | 9.98         | 16.49        | 0.7        | 2.5        | 39.6       | 0.00       | 2168     |
| dualwrite/inject  | 3    | 50000   | 16   | 0     | 6.01         | 8.39         | 12.37        | 0.6        | 1.9        | 72.7       | 0.00       | 1497     |
| outbox/control    | 3    | 50000   | 0    | 0     | 7.60         | 10.47        | 13.96        | 9.0        | 22.4       | 86.5       | 0.00       | 2025     |
| outbox/inject     | 3    | 50000   | 0    | 0     | 6.77         | 9.43         | 12.62        | 8114.5     | 18090.0    | 18341.4    | 8.19       | 2259     |

Per-repetition detail:

| arm       | mode    | rep | written | received | distinct | lost | dupes | publish errors |
| --------- | ------- | --- | ------- | -------- | -------- | ---- | ----- | -------------- |
| dualwrite | control | 2   | 50000   | 50000    | 50000    | 0    | 0     | 0              |
| dualwrite | control | 3   | 50000   | 50000    | 50000    | 0    | 0     | 0              |
| dualwrite | control | 4   | 50000   | 50000    | 50000    | 0    | 0     | 0              |
| dualwrite | inject  | 2   | 50000   | 49984    | 49984    | 16   | 0     | 16             |
| dualwrite | inject  | 3   | 50000   | 49984    | 49984    | 16   | 0     | 16             |
| dualwrite | inject  | 4   | 50000   | 49984    | 49984    | 16   | 0     | 16             |
| outbox    | control | 2   | 50000   | 50000    | 50000    | 0    | 0     | 0              |
| outbox    | control | 3   | 50000   | 50000    | 50000    | 0    | 0     | 0              |
| outbox    | control | 4   | 50000   | 50000    | 50000    | 0    | 0     | 0              |
| outbox    | inject  | 2   | 50000   | 50000    | 50000    | 0    | 0     | 0              |
| outbox    | inject  | 3   | 50000   | 50000    | 50000    | 0    | 0     | 0              |
| outbox    | inject  | 4   | 50000   | 50000    | 50000    | 0    | 0     | 0              |
