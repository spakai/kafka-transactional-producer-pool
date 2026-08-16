# Specification: Agentic Fault Injection for the Kafka Transactional Producer

## 1. Purpose

Build a safety-bounded agentic test harness that selects, executes, and evaluates fault-injection experiments against the transactional Kafka producer pool.

The harness shall use observations from the current run to choose the next experiment from an approved catalog. It shall never grant an AI model direct access to Docker, the network, Kafka administration, or arbitrary shell commands. All mutations shall pass through deterministic policy checks and the existing `ChaosController` abstraction.

This specification extends specs 001, 002, 004, and 006. The deterministic scenarios in spec 004 remain the underlying fault primitives and release-gate tests.

## 2. Goals

- Discover producer failure modes that fixed scenario ordering may miss.
- Validate atomicity, idempotence, ordering, bounded execution, recovery, and resource safety.
- Adapt fault type, timing, target, and intensity using evidence collected during the run.
- Produce a replayable experiment plan and sufficient evidence for every conclusion.
- Stop automatically before the configured safety envelope is exceeded.
- Separate infrastructure, harness, producer, correctness, and SLO failures.

## 3. Non-Goals

- Unrestricted autonomous chaos in production.
- Generation or execution of arbitrary commands by a model.
- Modification of Kafka data, topic configuration, security policy, or production configuration.
- Destructive quorum-loss or permanent data-loss experiments.
- Treating an AI-generated explanation as a correctness oracle.
- Replacing deterministic regression scenarios or human approval for release decisions.

## 4. Definitions

- **Experiment:** One baseline, fault, healing, recovery, and verification cycle.
- **Campaign:** A bounded sequence of experiments against one disposable cluster.
- **Agent:** The planner that proposes the next experiment from an allow-listed action schema.
- **Policy engine:** Deterministic code that validates, rejects, or clamps an agent proposal.
- **Executor:** Deterministic code that applies an approved plan through `ChaosController`.
- **Oracle:** Deterministic code that evaluates correctness, safety, and SLO assertions.
- **Blast radius:** Maximum permitted affected brokers, network paths, load, duration, and concurrency.
- **Novel failure:** A failure with a previously unseen normalized signature or state-transition path.
- **Ambiguous commit:** A commit whose outcome was not conclusively observed by the producer.

## 5. Safety Model

### 5.1 Environment authorization

Before every campaign and experiment, the harness shall:

- Require `--agentic-enabled=true` and `--chaos-enabled=true`.
- Match the observed Kafka cluster ID to `--cluster-allowlist`.
- Require an environment label equal to `disposable` or `ci-chaos`.
- Verify that all bootstrap servers, broker containers, topics, and fault targets are allow-listed.
- Reject unknown controllers, commands, actions, and parameters.
- Refuse execution if cluster inspection or cleanup verification is unavailable.

Production execution is prohibited in the first implementation. A future production mode requires a separate specification and approval mechanism.

### 5.2 Blast-radius limits

Default limits shall be:

| Limit | Default |
|---|---:|
| Campaign duration | 30 minutes |
| Experiments per campaign | 8 |
| Concurrent faults | 1 |
| Brokers stopped or isolated | 1 |
| Maximum fault duration | 60 seconds |
| Maximum load | 2x measured baseline |
| Consecutive unhealthy experiments | 2 |
| Recovery timeout | 120 seconds |

Limits may only be made stricter at runtime. Expanding them requires version-controlled configuration and human approval before the campaign begins.

### 5.3 Abort conditions

The safety monitor shall immediately stop load, heal active faults, and end the campaign when any of these occurs:

- Cleanup cannot be confirmed.
- The cluster identity or broker inventory changes unexpectedly.
- More than one broker becomes unavailable or any partition goes offline.
- The transaction-state topic or test topic loses its configured ISR safety margin.
- Publish calls exceed the configured hard timeout envelope.
- Heap, live threads, open file descriptors, or outstanding requests exceed configured ceilings.
- The pool remains `UNAVAILABLE` beyond the abort grace period.
- The control plane loses telemetry for longer than 15 seconds.
- A correctness oracle reports partial committed transactions or a visible conclusively aborted transaction.

Abort logic shall be local and deterministic; it shall not depend on an AI response.

## 6. Architecture

```text
Telemetry Snapshot -> Feature Extractor -> Agent Planner -> Proposed Experiment
                                                        |
                                                        v
Correctness Oracle <- Result Collector <- Executor <- Policy Engine
        |                                               |
        +---------- campaign history / next turn <------+
```

### 6.1 Agent planner

The planner receives only structured, redacted input:

- Campaign objective and remaining budgets.
- Allowed action schema and parameter bounds.
- Current cluster, producer-pool, JVM, and load features.
- Prior experiment plans, outcomes, and normalized failure signatures.
- Candidate hypotheses that remain untested.

The planner shall return one schema-valid proposal. Free-form text may explain the hypothesis but shall not control execution.

### 6.2 Policy engine

The policy engine shall independently validate:

- Action and target are in the catalog and allow-list.
- Fault count, duration, load, and campaign budgets remain within limits.
- The proposal has a registered cleanup action.
- The target exists and does not violate quorum or ISR rules.
- Required baseline evidence and observability are present.
- The experiment is materially distinct from prior experiments unless it is an explicit confirmation run.

A rejected proposal shall not be partially executed. The rejection and reason shall be recorded, and the planner may propose a replacement within its proposal budget.

### 6.3 Executor and safety monitor

The executor shall compile approved proposals into typed calls to `ChaosController`. It shall register cleanup before injection, confirm that the fault took effect, enforce deadlines, and emit timestamped `ChaosEvent` records.

The safety monitor shall run independently of the planner and executor. It shall retain authority to abort and heal at all times.

### 6.4 Deterministic oracles

Pass/fail status shall be computed from code and recorded evidence. The agent may summarize or recommend follow-up experiments but shall not override an oracle result.

Required oracles are:

- Publish-ledger correctness.
- Transaction atomicity and duplicate detection.
- Per-key ordering.
- Ambiguous-commit handling.
- Timeout and bounded-resource enforcement.
- Pool-state and lease reconciliation.
- Recovery and throughput SLOs.
- Fault application and cleanup verification.

## 7. Experiment Action Schema

The planner output shall be equivalent to:

```json
{
  "hypothesis": "Leader loss while the pool is saturated may delay recovery",
  "fault": {
    "type": "STOP_LEADER_BROKER",
    "targetPartition": 0,
    "startAfterSeconds": 30,
    "durationSeconds": 30
  },
  "load": {
    "threads": 12,
    "recordsPerTransaction": 10,
    "rateLimitTransactionsPerSecond": 500
  },
  "observeForSeconds": 90,
  "expectedSignals": ["pool_health", "transaction_outcome_total"],
  "stopConditions": ["offline_partitions > 0"]
}
```

The initial allow-listed fault types are:

- `NONE`.
- `STOP_NON_LEADER_BROKER`.
- `STOP_LEADER_BROKER`.
- `PARTITION_PRODUCER_FROM_ONE_BROKER`.
- `PARTITION_PRODUCER_FROM_CLUSTER`.
- `FLAP_PRODUCER_NETWORK`.
- `DROP_COMMIT_RESPONSE`.

The executor shall map these values to the existing spec 004 scenarios and controller methods. Unknown JSON fields, targets, or enum values shall cause rejection.

## 8. Campaign Lifecycle

1. **Authorize:** validate opt-ins, cluster identity, environment, and limits.
2. **Inspect:** record Kafka, topic, broker, pool, JVM, and controller state.
3. **Calibrate:** run a no-fault baseline and establish throughput and latency bands.
4. **Propose:** ask the planner for one hypothesis-driven experiment.
5. **Approve:** validate the proposal through the policy engine.
6. **Execute:** start bounded load, inject and confirm the fault, then heal it.
7. **Observe:** continue measurement until recovery or timeout.
8. **Verify:** stop publishers and run all deterministic oracles.
9. **Learn:** normalize the result and update campaign history.
10. **Continue or stop:** choose another experiment only if budgets and safety conditions permit.
11. **Replay:** rerun each novel failure once using its saved deterministic plan.
12. **Report:** persist evidence, decisions, cleanup status, and replay results.

Every experiment shall begin and end with zero active fault rules. A campaign shall fail if this cannot be proven.

## 9. Required Hypothesis Families

The agent shall cover these families before optimizing for novelty:

### AF-01: Healthy baseline and load boundary

Vary load within the approved range without injecting a fault. Identify saturation onset and verify bounded lease waits, backpressure errors, and stable resource usage.

### AF-02: Leadership and replica disruption

Select leader or non-leader loss based on current partition metadata. Compare availability, producer churn, leader-election delay, and ISR recovery.

### AF-03: Partial and complete connectivity loss

Select one-broker or whole-cluster producer isolation. Vary timing relative to transaction activity while keeping fault duration bounded.

### AF-04: Intermittent connectivity

Vary allow-listed flap count and healthy/fault intervals. Search for retry storms, repeated pool transitions, and recovery-supervisor contention.

### AF-05: Commit ambiguity

Inject a registered commit-response fault and verify that unknown commits are surfaced, affected producers are replaced, and callbacks are not automatically replayed.

### AF-06: Recovery under contention

Combine one allowed infrastructure fault with high but bounded publisher contention. Verify recovery concurrency, lease reconciliation, and time to steady state.

Only one fault primitive may be active at once. Combining load conditions with one fault is permitted.

## 10. Correctness and SLO Criteria

An experiment passes only when all applicable mandatory criteria pass:

- Every acknowledged transaction is fully visible to a `read_committed` consumer.
- No conclusively aborted transaction is visible to a `read_committed` consumer.
- No publish ID is committed more than once.
- No transaction is partially visible.
- Per-key committed ordering is preserved.
- Ambiguous outcomes are reported separately and are never automatically replayed.
- All publish calls finish within their configured timeout envelope.
- No producer lease, worker thread, or fault rule leaks.
- Ready plus leased producer counts reconcile with total healthy producers after recovery.
- The application recovers without restart.

Default recoverability SLOs are:

- First successful commit within 30 seconds after confirmed healing.
- Pool returns to `HEALTHY` within 60 seconds after cluster readiness.
- Throughput returns to at least 90% of the pre-fault median within 120 seconds.
- Post-recovery p95 latency returns to no more than 125% of baseline within 120 seconds.

Correctness criteria are immutable. SLO thresholds may be tuned only through reviewed configuration and shall be reported with every run.

## 11. Failure Classification

Every unsuccessful experiment shall receive exactly one primary classification:

- `AUTHORIZATION_FAILURE`: target or environment was not authorized.
- `PLANNER_FAILURE`: no schema-valid proposal was produced within the proposal budget.
- `POLICY_REJECTION`: the proposed action exceeded policy.
- `INJECTION_FAILURE`: the approved fault was not confirmed.
- `HARNESS_FAILURE`: load, telemetry, or verification infrastructure failed.
- `PRODUCER_CORRECTNESS_FAILURE`: an immutable correctness assertion failed.
- `PRODUCER_SAFETY_FAILURE`: execution or resources became unbounded.
- `PRODUCER_RECOVERY_FAILURE`: correctness held but recovery SLO failed.
- `CLEANUP_FAILURE`: the system could not prove that the fault was removed.

Planner or harness failures shall not be reported as producer failures.

## 12. Reproducibility and Audit

Each experiment shall persist:

- Campaign, run, and experiment IDs.
- Planner implementation and model identifier, when applicable.
- Hash of the planner instructions and action schema.
- Redacted planner input and complete structured proposal.
- Policy decision, applied clamps, and rejection reasons.
- Random seeds and full resolved configuration.
- Cluster ID, Kafka/client/JVM versions, topic metadata, and partition leadership.
- Fault application, confirmation, healing, and cleanup events.
- Per-second metrics, publish ledger, verification report, and failure signature.
- A deterministic replay file that does not require the agent.

Secrets, credentials, message payloads, and arbitrary logs shall not be sent to or stored in planner context.

## 13. Proposed Source Layout

```text
src/agentic/java/com/kafka/producer/agentic/
    AgenticCampaignRunner.java
    CampaignConfig.java
    ExperimentProposal.java
    ExperimentPolicy.java
    ExperimentExecutor.java
    SafetyMonitor.java
    FeatureExtractor.java
    FailureSignature.java
    ReplayWriter.java
    planner/
        ExperimentPlanner.java
        RuleBasedPlanner.java
        ModelBackedPlanner.java
    oracle/
        ExperimentOracle.java
        CorrectnessOracle.java
        RecoveryOracle.java
```

The first implementation shall include `RuleBasedPlanner` so the full workflow can be tested offline and deterministically. A model-backed planner shall implement the same interface and shall not be required for replay.

## 14. Command-Line Contract

The runner shall support arguments equivalent to:

```text
--agentic-enabled <true|false>
--chaos-enabled <true|false>
--cluster-allowlist <cluster-id>
--environment-label <label>
--planner <rule-based|model>
--campaign-duration-sec <n>
--max-experiments <n>
--max-fault-duration-sec <n>
--max-load-multiplier <n>
--replay <experiment.json>
--results-dir <path>
```

`--replay` shall bypass experiment selection but shall still require authorization and policy validation.

## 15. Test Strategy

### Unit tests

- Reject every unknown or out-of-range proposal field.
- Enforce cluster, topic, broker, duration, concurrency, and load allow-lists.
- Prove all abort conditions trigger cleanup.
- Validate stable failure-signature normalization.
- Verify planner input redaction.
- Verify replay serialization round-trips without semantic changes.

### Integration tests

- Run each allowed action against the disposable three-broker Docker cluster.
- Simulate planner timeout, invalid JSON, repeated proposals, and unavailable planner.
- Kill the runner during a fault and verify shutdown-hook and external watchdog cleanup.
- Disconnect telemetry and verify fail-closed behavior.
- Replay a saved experiment and compare its resolved action sequence.

### Campaign qualification

- Execute at least 20 seeded campaigns with `RuleBasedPlanner`.
- Execute model-backed campaigns only after all policy mutation tests pass.
- Confirm that invalid proposals never reach `ChaosController`.
- Confirm that every injected fault has confirmed cleanup evidence.

## 16. CI and Release Gating

- Pull requests shall run unit tests and rule-based dry runs with a fake controller.
- Scheduled CI shall run seeded rule-based campaigns on the disposable cluster.
- Model-backed campaigns shall run as non-blocking discovery jobs initially.
- A discovered issue becomes a release gate only after it is reproduced by a saved deterministic replay and converted into a stable regression scenario.
- Correctness or cleanup failures shall always fail the scheduled campaign job.

## 17. Definition of Done

The feature is complete when:

- The agent can select only schema-valid experiments from the approved catalog.
- The policy engine prevents every tested blast-radius and authorization violation.
- The safety monitor aborts and heals without planner participation.
- All required hypothesis families can be exercised and replayed deterministically.
- Correctness and recovery outcomes are decided exclusively by deterministic oracles.
- Novel failures produce minimized, replayable evidence suitable for a regression test.
- Campaign reports clearly distinguish planner, harness, injection, producer, and cleanup failures.
- No campaign finishes with an active fault, leaked lease, or unverified cleanup state.
