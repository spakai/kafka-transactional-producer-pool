package com.kafka.producer.agentic;

import com.kafka.producer.agentic.planner.ExperimentPlanner;
import com.kafka.producer.agentic.planner.RuleBasedPlanner;
import com.kafka.producer.chaos.ChaosConfig;
import com.kafka.producer.chaos.ChaosEvent;
import com.kafka.producer.chaos.ChaosLoadEngine;
import com.kafka.producer.chaos.ClusterInspector;
import com.kafka.producer.chaos.CorrectnessVerifier;
import com.kafka.producer.chaos.EventRecorder;
import com.kafka.producer.chaos.PublishLedger;
import com.kafka.producer.chaos.VerificationReport;
import com.kafka.producer.chaos.controller.DockerChaosController;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public final class AgenticCampaignRunner {
    private AgenticCampaignRunner() {}

    public static void main(String[] args) throws Exception {
        CampaignConfig config = CampaignConfig.fromArgs(args);
        ExperimentPlanner planner = new RuleBasedPlanner(config);
        if (config.dryRun()) {
            dryRun(config, planner);
            return;
        }
        config.requireAuthorization();
        String campaignId = "campaign-" + UUID.randomUUID();
        EventRecorder events = new EventRecorder();
        List<ExperimentResult> results = new ArrayList<>();
        List<String> completedFamilies = new ArrayList<>();
        Set<String> signatures = new HashSet<>();
        long campaignStarted = System.nanoTime();

        try (ClusterInspector inspector = new ClusterInspector(config.bootstrapServers(), Duration.ofSeconds(15))) {
            ClusterInspector.ClusterSnapshot initial = inspector.inspect(config.topic());
            verifyEnvironment(config, initial);
            while (results.size() < config.maxExperiments()
                    && elapsed(campaignStarted).compareTo(config.campaignDuration()) < 0) {
                ClusterInspector.ClusterSnapshot snapshot = inspector.inspect(config.topic());
                ExperimentPlan plan;
                if (config.replayFile() != null) {
                    if (!results.isEmpty()) break;
                    plan = validateReplay(config, ReplayCodec.read(config.replayFile()), snapshot,
                            elapsed(campaignStarted), signatures);
                } else {
                    ExperimentProposal proposal = planner.propose(snapshot, completedFamilies);
                    if (proposal == null) break;
                    PolicyDecision decision = new ExperimentPolicy().evaluate(config, proposal, snapshot,
                            results.size(), elapsed(campaignStarted), signatures);
                    if (!decision.approved()) {
                        events.accept(ChaosEvent.of("policy", "rejected", String.join(",", decision.reasons())));
                        completedFamilies.add(proposal.hypothesisFamily());
                        continue;
                    }
                    plan = decision.plan();
                }
                events.accept(ChaosEvent.of("policy", "approved", plan.experimentId()));
                ExperimentResult result = runExperiment(config, plan, inspector, events);
                results.add(result);
                signatures.add(ExperimentPolicy.signature(plan));
                completedFamilies.add(plan.hypothesisFamily());
                if (result.failureClass() == FailureClass.CLEANUP_FAILURE
                        || result.failureClass() == FailureClass.PRODUCER_CORRECTNESS_FAILURE
                        || result.failureClass() == FailureClass.PRODUCER_SAFETY_FAILURE) break;
                inspector.awaitBrokerCount(config.topic(), 3, config.recoveryTimeout());
                inspector.awaitFullyReplicated(config.topic(), config.recoveryTimeout());
            }
        }
        PathResult output = write(config, campaignId, planner.identity(), results, events.snapshot());
        System.out.println("Agentic campaign completed: " + output.path());
        if (results.stream().anyMatch(r -> !r.passed())) {
            throw new IllegalStateException("Campaign contained failed experiments; see " + output.path());
        }
    }

    private static ExperimentResult runExperiment(CampaignConfig campaign, ExperimentPlan plan,
            ClusterInspector inspector, EventRecorder events) throws Exception {
        String runId = plan.experimentId() + '-' + UUID.randomUUID();
        ChaosConfig chaos = ChaosConfigAdapter.create(campaign, plan,
                campaign.resultsDirectory().toString());
        PublishLedger ledger = new PublishLedger();
        List<ChaosLoadEngine.Sample> samples = new ArrayList<>();
        SafetyMonitor monitor = new SafetyMonitor(campaign.clusterAllowlist());
        String safetyAbort = null;
        boolean cleanup = false;
        VerificationReport verification;
        try (DockerChaosController controller = new DockerChaosController(chaos, events);
             ExperimentExecutor executor = new ExperimentExecutor(plan, controller, events);
             ChaosLoadEngine load = new ChaosLoadEngine(chaos, runId, ledger)) {
            load.start();
            long started = System.nanoTime();
            while (elapsed(started).compareTo(plan.totalDuration()) < 0) {
                Duration elapsed = elapsed(started);
                executor.tick(elapsed);
                ClusterInspector.ClusterSnapshot cluster = inspectBestEffort(inspector, campaign.topic(), events);
                ChaosLoadEngine.Sample sample = load.sample(elapsed.toSeconds(), cluster);
                samples.add(sample);
                var abort = monitor.evaluate(cluster, sample);
                if (abort.isPresent()) { safetyAbort = abort.get(); break; }
                TimeUnit.SECONDS.sleep(1);
            }
            executor.heal();
            load.stopAndAwait();
            executor.verifyCleanup();
            cleanup = true;
        } catch (Exception error) {
            events.accept(ChaosEvent.of("experiment", "failed", error.toString()));
            if (!cleanup) {
                verification = emptyVerification("execution failed before verification: " + error);
                return new ExperimentResult(plan, FailureClass.CLEANUP_FAILURE, error.toString(), false, samples, verification);
            }
            throw error;
        }
        inspector.awaitBrokerCount(campaign.topic(), 3, campaign.recoveryTimeout());
        inspector.awaitFullyReplicated(campaign.topic(), campaign.recoveryTimeout());
        verification = new CorrectnessVerifier(chaos).verify(runId, ledger);
        return new ExperimentOracle().evaluate(plan, samples, verification, cleanup, safetyAbort);
    }

    private static ExperimentPlan validateReplay(CampaignConfig config, ExperimentPlan replay,
            ClusterInspector.ClusterSnapshot snapshot, Duration elapsed, Set<String> signatures) {
        ExperimentProposal proposal = new ExperimentProposal(replay.hypothesis(), replay.hypothesisFamily(),
                replay.faultType(), replay.targetPartition(), replay.startAfter(), replay.faultDuration(),
                replay.threads(), replay.recordsPerTransaction(), replay.observeFor(), replay.expectedSignals());
        PolicyDecision decision = new ExperimentPolicy().evaluate(config, proposal, snapshot, 0, elapsed, signatures);
        if (!decision.approved()) throw new IllegalStateException("Replay rejected: " + decision.reasons());
        return decision.plan();
    }

    private static void verifyEnvironment(CampaignConfig config, ClusterInspector.ClusterSnapshot snapshot) {
        if (!config.clusterAllowlist().equals(snapshot.clusterId())) throw new IllegalStateException("cluster ID mismatch");
        if (snapshot.brokerIds().size() != 3 || snapshot.offlinePartitions() > 0
                || snapshot.underReplicatedPartitions() > 0) throw new IllegalStateException("cluster is not fully healthy");
    }

    private static void dryRun(CampaignConfig config, ExperimentPlanner planner) {
        var leaders = new LinkedHashMap<TopicPartition, Integer>();
        leaders.put(new TopicPartition(config.topic(), config.targetPartition()), 1);
        String clusterId = config.clusterAllowlist().isBlank() ? "dry-run-cluster" : config.clusterAllowlist();
        var snapshot = new ClusterInspector.ClusterSnapshot(clusterId, List.of(1, 2, 3), leaders, 0, 0);
        ExperimentProposal proposal = planner.propose(snapshot, List.of());
        System.out.println("Dry-run proposal: " + proposal);
    }

    private static ClusterInspector.ClusterSnapshot inspectBestEffort(
            ClusterInspector inspector, String topic, EventRecorder events) {
        try { return inspector.inspect(topic); }
        catch (Exception e) { events.accept(ChaosEvent.of("telemetry", "unavailable", e.toString())); return null; }
    }
    private static Duration elapsed(long started) { return Duration.ofNanos(System.nanoTime() - started); }
    private static VerificationReport emptyVerification(String issue) {
        return new VerificationReport(0, 0, 0, 0, 0, 0, 0, 0, List.of(issue), List.of());
    }
    private static PathResult write(CampaignConfig c, String id, String planner,
            List<ExperimentResult> results, List<ChaosEvent> events) throws Exception {
        return new PathResult(new CampaignResultWriter().write(c, id, planner, results, events));
    }
    private record PathResult(java.nio.file.Path path) {}
}
