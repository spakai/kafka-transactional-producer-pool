# Chaos coverage assessment — 2026-10-23

Live runs were executed against the disposable three-broker Kafka 3.7.0 and
Toxiproxy stack. The IDs below identify run bundles in `chaos-results/` (MB-02
used `chaos-results-recheck/`). PASS verification rows report zero missing
committed records, duplicates, partial transactions, ordering violations, and
pending transactions. The two-topic runs also report zero cross-topic
mismatches. MB-01 checked aborted-record visibility under both isolation
levels.

## Live campaign results

| Scenario | Run configuration | Ledger outcomes | Record visibility and correctness | Status and artifact prefix |
|---|---|---|---|---|
| MB-01 | 120s, two topics, default abort sample every 10 publishes | 32,250 total: 29,029 committed; 3,221 aborted | 580,580 committed records in `read_committed`; 645,000 in `read_uncommitted`, including 64,420 aborted records; 0 missing, duplicate, partial, or cross-topic mismatch | **PASS** — `MB-01-2026-10-23T04-42-06.359142003Z` |
| MB-02 | 30s, round-robin across 12 partitions on two topics | 8,569 committed | 171,380 records under each isolation level; all 24 topic partitions sampled; 0 ordering violations | **PASS** — `MB-02-2026-10-23T04-44-16.741371760Z` in `chaos-results-recheck/` |
| CH-01 | 180s; broker 1 restarted with preferred leader counts 6/3/3 | 46,156 committed | 923,120 records under each isolation level; 0 missing or duplicate records; broker restart and healthy recovery confirmed | **PASS** — `CH-01-2026-10-23T05-01-21.160003912Z` |
| CH-04 | 215s; six partition/heal cycles; 2 workers | 30,516 committed | 305,160 records under each isolation level; 0 missing, duplicate, partial, or cross-topic records; 6/6 faults healed; measured blocked windows 9.6–11.6s and five inter-cycle healthy windows 20.2–21.6s; cleanup confirmed | **PASS** — `CH-04-2026-10-23T06-35-12.631653134Z` |
| MB-03 | 60s; non-leader replica stopped for 20s | 15,403 committed | 154,030 records under each isolation level; broker restart and recovery confirmed | **PASS** — `MB-03-2026-10-23T06-13-14.625158912Z` |
| MB-04 | 60s; target partition leader stopped for 20s | 16,270 committed | 162,700 records under each isolation level; broker restart and recovery confirmed | **PASS** — `MB-04-2026-10-23T06-14-27.406718152Z` |
| CH-02 | 60s; producer blocked from one leader broker for 20s | 11,300 total: 11,299 committed; 1 ambiguous | 112,990 committed records; 113,000 uncommitted-visible records; no missing acknowledged or duplicate records; network heal confirmed | **PASS** — `CH-02-2026-10-23T06-15-35.404673402Z` |
| CH-03 | 60s; producer blocked from all brokers for 20s | 8,180 total: 8,172 committed; 8 ambiguous | 81,720 committed records; 81,780 uncommitted-visible records; no missing acknowledged or duplicate records; network heal confirmed | **PASS** — `CH-03-2026-10-23T06-16-43.386937395Z` |
| CH-05 | 60s TCP response-drop approximation for 20s | 8,132 total: 8,124 committed; 8 ambiguous | 81,240 committed records; 81,310 uncommitted-visible records; network heal confirmed, but no proof of correlated `EndTxn` request/response loss | **INVALID/INCOMPLETE** — `CH-05-2026-10-23T06-17-54.870349108Z` |

For every run bundle, inspect the matching `-verification.csv` for per-publish
visibility, `-offsets.csv` for captured bounds, `-partition-counts.csv` for
partition totals, `-events.jsonl` for fault/recovery events, and `.csv` for
time-series pool and throughput samples. `chaos-results/summary.md` contains
numeric run summaries. CH-05 intentionally exits Maven with a nonzero status
after writing its `INVALID/INCOMPLETE` bundle.

## Harness defects exposed by live runs

The first full MB-01 attempt found 3,034 intentional aborts but 0 corresponding
records in `read_uncommitted`; the producer had locally queued those sends when
the harness aborted. The deliberate abort path now waits for send acknowledgments
before aborting. The corrected full run observed all 64,420 expected aborted
records under `read_uncommitted` and none under `read_committed`.

The first MB-02 attempt reported 12,533 ordering inversions because the verifier
compared sequence numbers across partitions. Kafka ordering is per partition;
the verifier now groups by topic, partition, and key. The corrected MB-02 run
reported 0 ordering violations and sampled all 24 configured partitions.

CH-04 also exposed timing drift. Broker metadata requests stalled while the
producer was partitioned, stretching the first flap windows to roughly 15s.
The runner now skips those requests during intentional full partitions, uses a
fixed sample cadence, and starts each transition deadline after the previous
fault/heal command completes. The final event timestamps show six blocked
windows of 9.6–11.6s and five inter-cycle healthy intervals of 20.2–21.6s.

Those initial failing artifacts remain in `chaos-results/summary.md` as
diagnostics; the later corrected run bundles above are the valid MB-01/MB-02
results.

## Historical AF-05 interpretation

`campaign-e6ddeb88-5e44-4359-b404-3dbefa0ca433` remains unchanged as a historical
artifact. It records 13,464 attempted/committed transactions, 0 failed, and 0
ambiguous; the event log records a telemetry timeout during the fault window.
Its earlier `PASSED` label came from an oracle that did not require protocol
proof or an observed ambiguous result. It is **INVALID/INCOMPLETE CH-05 evidence**,
not a successful commit-response-loss test.

## Validation and cleanup

- `mvn -Pchaos -Dtest=ClusterInspectorTest,ChaosRunnerTest,ChaosConfigTest,ChaosResultWriterTest test` — 10 tests passed after the verifier fixes.
- `mvn -Pchaos,agentic -Dtest=ClusterInspectorTest,ChaosRunnerTest,ChaosConfigTest,ChaosResultWriterTest,PublishLedgerTest,ExperimentOracleTest,CampaignConfigTest,CampaignResultWriterTest,SafetyMonitorTest test` — 19 focused tests passed before the live-run fixes.
- The earlier full `mvn -Pchaos,agentic test` had 19 Mockito/Byte Buddy self-attach errors in this WSL/JDK environment.
- After the campaigns, all three brokers and Toxiproxy were healthy, and each proxy reported 0 active toxics.
- CH-05 still needs a protocol-aware proxy that records `EndTxn` and drops its correlated response, with broker transaction-state evidence, to become a valid commit-response-loss test.
