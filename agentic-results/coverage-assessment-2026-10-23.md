# Chaos coverage assessment — 2026-10-23

This assessment separates harness changes from executed Kafka evidence. The
Docker CLI is unavailable in the current WSL environment, so no new broker or
network chaos run was executed for this update. No result is inferred from a
unit test or marked passed without a live run.

| Coverage | Harness / evidence change | Current result status |
|---|---|---|
| MB-01 baseline and abort visibility | Intentional abort control sample is enabled every ten publishes by default. Verification reads all configured output topics under both isolation levels and uses captured offset bounds. | **NOT RUN** in this environment; Docker unavailable. |
| MB-02 partition distribution | Load routes publishes round-robin across `--partition-count`; verifier records per-partition committed counts and fails when any configured partition has no sample. | **NOT RUN** in this environment; Docker unavailable. |
| CH-01 leader-heavy restart | `prepare-ch01` creates a dedicated 12-partition topic with preferred leader counts 6/3/3 and runs preferred leader election. Scenario selection still validates a strict live imbalance before stopping a broker. | **NOT RUN** in this environment; Docker unavailable. |
| CH-04 flapping network | Existing six-cycle scenario remains; use a run duration that includes all 180 seconds of cycles plus recovery. | **NOT RUN** in this environment; Docker unavailable. |
| Numeric correctness evidence | Direct and agentic output now persist verifier totals, per-publish rows, offset ranges, and per-partition committed counts. | Code compiled and focused tests passed; integration evidence awaits a live cluster. |
| CH-05 commit response loss | Agentic oracle now classifies the current Toxiproxy-only `DROP_COMMIT_RESPONSE` experiment as `INVALID_INCOMPLETE` after checking correctness and recovery. | **NOT QUALIFIED.** Protocol-aware correlated `EndTxn` interception is still required for a valid pass. |

## Historical AF-05 interpretation

`campaign-e6ddeb88-5e44-4359-b404-3dbefa0ca433` remains unchanged as a historical
artifact. It records 13,464 attempted/committed transactions, 0 failed, and 0
ambiguous; the event log records a telemetry timeout during the fault window.
Its earlier `PASSED` label came from an oracle that did not require protocol
proof or an observed ambiguous result. Under the current acceptance criteria,
it is **INVALID/INCOMPLETE CH-05 evidence**, not a successful commit-response
loss test.

## Verification performed for this update

- `mvn -Pchaos,agentic -DskipTests compile` — passed.
- `mvn -Pchaos,agentic -Dtest=ClusterInspectorTest,ChaosRunnerTest,ChaosConfigTest,ChaosResultWriterTest,PublishLedgerTest,ExperimentOracleTest,CampaignConfigTest,CampaignResultWriterTest,SafetyMonitorTest test` — 19 tests passed.
- `mvn -Pchaos,agentic test` — chaos and agentic tests ran, but the full suite had 19 errors in existing Mockito-based pool tests because Byte Buddy could not self-attach in this WSL/JDK environment; no assertion failures were reported.
- `./scripts/chaos-cluster.sh status` — not run successfully: `docker` is unavailable in this WSL 2 distro.
