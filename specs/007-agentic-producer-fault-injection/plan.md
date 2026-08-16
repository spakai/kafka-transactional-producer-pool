# Implementation Plan: Agentic Producer Fault Injection

## 1. Objective

Implement the agentic fault-injection system defined in [spec.md](spec.md) as a safety-bounded extension of the existing Kafka producer chaos harness.

The implementation shall deliver a deterministic campaign runner first. Model-backed experiment selection shall be added only after authorization, policy enforcement, cleanup, safety monitoring, correctness oracles, and replay have automated coverage.

## 2. Delivery Principles

- Reuse `ChaosController`, `ChaosEvent`, `ChaosLoadEngine`, `PublishLedger`, and `CorrectnessVerifier` where their contracts are sufficient.
- Keep agentic orchestration outside `src/main`; it must not add runtime dependencies to the producer-pool library.
- Represent every planner decision as typed data before it reaches fault-control code.
- Keep authorization, policy, abort handling, and pass/fail decisions deterministic.
- Register and verify cleanup before executing any fault.
- Make every executed experiment replayable without a planner or model.
- Add tests with each milestone rather than deferring verification to the end.

## 3. Proposed Package Layout

```text
src/agentic/java/com/kafka/producer/agentic/
    AgenticCampaignRunner.java
    CampaignConfig.java
    CampaignContext.java
    CampaignResult.java
    ExperimentProposal.java
    ExperimentPlan.java
    ExperimentPolicy.java
    PolicyDecision.java
    ExperimentExecutor.java
    ExperimentResult.java
    SafetyMonitor.java
    FeatureExtractor.java
    ObservationSnapshot.java
    FailureSignature.java
    ReplayWriter.java
    ReplayReader.java
    planner/
        ExperimentPlanner.java
        RuleBasedPlanner.java
        ModelBackedPlanner.java
    oracle/
        ExperimentOracle.java
        OracleResult.java
        CorrectnessOracle.java
        ResourceSafetyOracle.java
        RecoveryOracle.java

src/agentic/test/com/kafka/producer/agentic/
    CampaignConfigTest.java
    ExperimentProposalTest.java
    ExperimentPolicyTest.java
    ExperimentExecutorTest.java
    SafetyMonitorTest.java
    FeatureExtractorTest.java
    FailureSignatureTest.java
    ReplayTest.java
    AgenticCampaignRunnerTest.java
```

The final layout may consolidate small value types, but planner, policy, execution, safety, and oracle boundaries shall remain explicit.

## 4. Milestones

### Milestone 1: Build and domain foundation

**Goal:** establish the source set, configuration contract, and typed experiment model without executing faults.

Tasks:

1. Add an `agentic` Maven profile and source/test source sets following the existing `chaos`, `baseline`, and `observability` patterns.
2. Add `CampaignConfig` with CLI parsing for:
   - Agentic and chaos opt-ins.
   - Cluster allow-list and environment label.
   - Campaign duration and experiment count.
   - Fault-duration, load, proposal, recovery, and abort limits.
   - Planner selection, results directory, and replay path.
3. Define enums and immutable types for fault type, load settings, expected signals, stop conditions, and experiment proposal.
4. Separate an untrusted `ExperimentProposal` from a resolved, approved `ExperimentPlan`.
5. Reject unknown fields, invalid enum values, negative values, invalid combinations, and numeric overflow.
6. Add configuration and proposal-schema unit tests.

Deliverables:

- The `agentic` profile compiles independently.
- Valid proposals round-trip through the selected serialization format.
- Invalid inputs fail before any controller is constructed.

Exit criteria:

- `mvn -Pagentic test` runs the new tests.
- No agentic dependency is present in the default production artifact.
- Proposal types cannot directly contain shell commands or arbitrary controller operations.

### Milestone 2: Authorization and policy engine

**Goal:** make unsafe proposals impossible to execute through the supported path.

Tasks:

1. Implement campaign-level authorization checks for both opt-ins, environment label, cluster ID, bootstrap servers, topic, broker inventory, and controller type.
2. Implement `ExperimentPolicy` as a pure deterministic evaluator.
3. Validate fault type, target, duration, observation window, concurrency, load multiplier, campaign budget, recovery state, and required telemetry.
4. Check current leaders, ISR state, offline partitions, and broker availability before approving broker faults.
5. Require a known cleanup mapping for every mutable action.
6. Support only rejection or safe clamping explicitly permitted by configuration; record all clamps.
7. Add table-driven tests for every rejection rule and boundary value.

Deliverables:

- `PolicyDecision` reports approval, resolved plan, clamps, or stable rejection codes.
- Fake proposals cannot bypass quorum, budget, target, or cleanup controls.

Exit criteria:

- Every allow-listed fault has positive and negative policy tests.
- Mutation tests or equivalent adversarial test cases demonstrate that invalid proposals never reach the controller.

### Milestone 3: Executor and safety monitor

**Goal:** execute one approved experiment safely and guarantee best-effort healing on every exit path.

Tasks:

1. Implement `ExperimentExecutor` using typed `ChaosController` calls only.
2. Register cleanup before injection and persist the intended cleanup event.
3. Confirm injection took effect before entering the fault observation window.
4. Enforce start, fault, observation, recovery, and total experiment deadlines.
5. Implement `SafetyMonitor` on an independent scheduler and state path.
6. Add abort conditions for cluster identity, broker count, offline partitions, ISR margin, pool availability, telemetry loss, resource ceilings, and hard call deadlines.
7. On abort, stop load, invoke cleanup, verify cleanup, and prevent further experiments.
8. Add shutdown-hook behavior and an external cleanup-marker contract for abnormal process termination.
9. Test with a fake controller that can delay, fail, partially apply, and fail cleanup.

Deliverables:

- One approved no-fault or single-fault experiment can run end to end.
- All state transitions and controller operations emit timestamped events.

Exit criteria:

- No tested exception path skips cleanup.
- Cleanup failure is reported distinctly and terminates the campaign.
- The safety monitor can abort without calling the planner.

### Milestone 4: Observations and deterministic oracles

**Goal:** evaluate experiments using recorded evidence rather than planner judgment.

Tasks:

1. Add `ObservationSnapshot` for synchronized cluster, producer-pool, load, JVM, and correctness features.
2. Reuse or adapt `PublishLedger` and `CorrectnessVerifier` for experiment-scoped verification.
3. Implement `CorrectnessOracle` for missing acknowledgements, visible failures, duplicates, partial transactions, ordering violations, and ambiguous outcomes.
4. Implement `ResourceSafetyOracle` for leases, threads, heap, outstanding calls, timeout envelopes, and remaining fault rules.
5. Implement `RecoveryOracle` for first successful commit, pool health, throughput restoration, and latency restoration.
6. Combine oracle results without allowing one oracle to overwrite another.
7. Define stable primary failure classification and retain all secondary findings.

Deliverables:

- An `ExperimentResult` contains evidence, individual oracle results, primary classification, and cleanup status.
- Correctness failures immediately trigger the campaign abort path.

Exit criteria:

- Unit tests cover every mandatory correctness assertion.
- Planner text or output has no route to alter an oracle result.

### Milestone 5: Deterministic campaign runner

**Goal:** deliver the first usable adaptive campaign runner without an external model.

Tasks:

1. Define `ExperimentPlanner` with structured input and one-proposal output.
2. Implement a seeded `RuleBasedPlanner` covering AF-01 through AF-06.
3. Implement campaign phases: authorize, inspect, calibrate, propose, approve, execute, observe, verify, learn, replay, and report.
4. Track remaining time, experiment, proposal, fault, and unhealthy-result budgets.
5. Reject repeated experiments unless marked as confirmation or replay.
6. Stop after all required hypothesis families, budget exhaustion, abort, or cleanup failure.
7. Add fake-controller campaign tests with deterministic seeds.

Deliverables:

- A complete campaign can run offline with a deterministic sequence of proposals.
- The rule-based planner exercises every permitted action through the same policy boundary intended for a model.

Exit criteria:

- Repeating a campaign with identical inputs and seed produces the same resolved plans.
- The runner never starts the next experiment unless the previous cleanup is confirmed.

### Milestone 6: Evidence, failure signatures, and replay

**Goal:** make findings auditable and reproducible without agent participation.

Tasks:

1. Write campaign metadata, resolved configuration, environment metadata, and planner identity.
2. Persist redacted planner input, proposal, policy decision, events, samples, ledger, oracle results, and cleanup evidence.
3. Normalize failure signatures using failure class, exception family, pool transition path, affected operation, and failed oracle IDs.
4. Write a deterministic replay file containing the fully resolved plan and seed.
5. Implement replay loading, authorization, policy revalidation, and execution.
6. Replay each novel producer failure once before marking it reproducible.
7. Produce a Markdown campaign summary and machine-readable JSON/CSV artifacts.

Suggested output layout:

```text
agentic-results/<campaign-id>/
    campaign.json
    summary.md
    events.jsonl
    experiments/<experiment-id>/
        proposal.json
        policy.json
        resolved-plan.json
        replay.json
        samples.csv
        verification.csv
        result.json
```

Exit criteria:

- A replay requires no planner implementation or model credentials.
- Serialized plans round-trip without changing their action sequence.
- Reports distinguish planner, policy, injection, harness, producer, and cleanup failures.

### Milestone 7: Model-backed planner

**Goal:** add adaptive model selection without expanding execution authority.

Tasks:

1. Implement `ModelBackedPlanner` behind `ExperimentPlanner`.
2. Send only structured, redacted observations, history summaries, remaining budgets, and the action schema.
3. Require schema-constrained structured output.
4. Add proposal timeout, size limit, retry budget, and fail-closed parsing.
5. Record model identity and hashes of instructions and schema.
6. Treat invalid, repeated, unsafe, or unavailable responses as planner failures or policy rejections, never as executable fallbacks.
7. Add contract tests using recorded responses; keep live-model tests optional and non-blocking.

Exit criteria:

- Model proposals pass through the same parser and policy engine as rule-based proposals.
- The model receives no credentials, payloads, raw arbitrary logs, or command execution capability.
- Model unavailability cannot prevent cleanup or leave an experiment running.

### Milestone 8: Docker integration and resilience qualification

**Goal:** validate the system against the disposable three-broker cluster.

Tasks:

1. Run each allow-listed fault independently through the new executor.
2. Run AF-01 through AF-06 with fixed seeds and bounded durations.
3. Verify injection confirmation, recovery measurement, correctness scans, and cleanup proof.
4. Terminate the runner during baseline, injection, fault, healing, and verification phases and validate recovery behavior.
5. Test telemetry loss, stale cluster metadata, controller timeout, planner timeout, disk-write failure, and failed cleanup.
6. Run at least 20 seeded rule-based qualification campaigns.
7. Run model-backed discovery campaigns only after deterministic qualification passes.

Exit criteria:

- Every injected fault has matching confirmed cleanup evidence.
- No qualification campaign leaks fault rules, producers, leases, or worker threads.
- Any model-discovered issue is reproducible through a deterministic replay before it becomes a regression test.

### Milestone 9: Documentation and CI

**Goal:** make the harness safe and repeatable for developers and scheduled automation.

Tasks:

1. Document prerequisites, safety constraints, CLI arguments, dry-run, campaign, and replay commands.
2. Add a fake-controller dry-run job for pull requests.
3. Add seeded rule-based Docker campaigns to scheduled CI.
4. Publish all reports and cleanup evidence as build artifacts.
5. Add model-backed campaigns as non-blocking scheduled discovery jobs.
6. Document triage and the process for turning a replay into a fixed deterministic regression scenario.

Exit criteria:

- A developer can run a safe rule-based campaign and replay using documented commands.
- CI does not run privileged fault injection for untrusted contributions.
- Correctness and cleanup failures fail scheduled qualification jobs.

## 5. Dependency Order

```text
Domain model
    -> Policy engine
        -> Executor + safety monitor
            -> Observations + oracles
                -> Rule-based campaign runner
                    -> Evidence + replay
                        -> Model-backed planner
                            -> Docker qualification + CI
```

The model-backed planner is intentionally late in the dependency chain. It must not be used to compensate for missing policy, safety, verification, or cleanup behavior.

## 6. Testing Matrix

| Layer | Primary test type | Required focus |
|---|---|---|
| Configuration and proposal | Unit | Parsing, bounds, unknown fields, overflow |
| Policy | Table-driven unit | Every allow and deny rule |
| Executor | Fake controller | Deadlines, partial failure, cleanup |
| Safety monitor | Unit and concurrency | Independent abort and race handling |
| Oracles | Unit and fixtures | Atomicity, duplicates, ordering, recovery |
| Campaign runner | Fake controller | Budgets, determinism, lifecycle |
| Replay | Round-trip and integration | Identical resolved action sequence |
| Fault primitives | Docker integration | Injection confirmation and healing |
| Full campaign | Scheduled Docker | Safety, correctness, recovery, artifacts |
| Model planner | Contract and optional live | Redaction, schema, timeout, invalid output |

## 7. Initial Work Breakdown

The first development slice shall be small enough to review independently:

1. Add the Maven source-set/profile wiring.
2. Implement `CampaignConfig`, `ExperimentProposal`, and fault/load enums.
3. Implement strict proposal parsing and validation.
4. Add unit tests for defaults, required authorization fields, valid proposals, and malformed proposals.
5. Add a dry-run entry point that parses and prints a normalized proposal without constructing a chaos controller.

The second slice shall implement the policy engine and fake cluster state. No real fault shall be executable until that slice is complete and tested.

## 8. Risks and Mitigations

- **Risk:** Existing chaos classes combine orchestration and scenario behavior too tightly for reuse.
  - **Mitigation:** Add adapters in the agentic source set; avoid changing the production pool API.
- **Risk:** Cleanup relies only on an in-process shutdown hook.
  - **Mitigation:** Persist cleanup intent before injection and provide an external cleanup/reconciliation command.
- **Risk:** Telemetry lag causes unsafe or incorrect agent decisions.
  - **Mitigation:** Timestamp observations, reject stale snapshots, and make telemetry loss an abort condition.
- **Risk:** Model output changes between runs.
  - **Mitigation:** Save resolved plans and require deterministic replay for confirmed findings.
- **Risk:** Planner novelty produces redundant tests instead of useful coverage.
  - **Mitigation:** Cover required hypothesis families first and normalize prior plan signatures.
- **Risk:** A harness defect is misclassified as a producer defect.
  - **Mitigation:** Use explicit failure classes and require confirmed injection, telemetry, verification, and cleanup evidence.

## 9. Completion Checklist

- [ ] Agentic code is isolated from the production artifact.
- [ ] Both chaos and agentic opt-ins are mandatory.
- [ ] Cluster and target allow-lists are enforced before every experiment.
- [ ] Every mutable action has registered and verified cleanup.
- [ ] Safety monitoring operates independently of the planner.
- [ ] All correctness decisions come from deterministic oracles.
- [ ] AF-01 through AF-06 are covered by the rule-based planner.
- [ ] Every experiment emits a deterministic replay file.
- [ ] Model inputs are structured and redacted.
- [ ] Model output cannot bypass proposal parsing or policy checks.
- [ ] Docker qualification covers every allowed fault and forced interruption phase.
- [ ] Scheduled CI publishes reports and cleanup evidence.
- [ ] The README documents safe execution and replay.

## 10. Recommended First Release Boundary

Version one should include milestones 1 through 6 and 8 through 9 using `RuleBasedPlanner`. This provides adaptive, state-aware experiment sequencing, deterministic safety controls, complete verification, and replay without introducing an external model dependency.

`ModelBackedPlanner` should ship initially as an opt-in discovery capability after the deterministic release has passed qualification. It should not become a release gate until its findings are consistently reproducible through saved replay plans.
