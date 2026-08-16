package com.kafka.producer.agentic;

import com.kafka.producer.chaos.ChaosEvent;
import com.kafka.producer.chaos.ChaosLoadEngine;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

final class CampaignResultWriter {
    Path write(CampaignConfig config, String campaignId, String planner,
               List<ExperimentResult> results, List<ChaosEvent> events) throws IOException {
        Path root = config.resultsDirectory().resolve(campaignId);
        Files.createDirectories(root.resolve("experiments"));
        StringBuilder summary = new StringBuilder("# Agentic campaign ").append(campaignId).append("\n\n")
                .append("- Planner: `").append(planner).append("`\n")
                .append("- Seed: `").append(config.seed()).append("`\n")
                .append("- Cluster: `").append(config.clusterAllowlist()).append("`\n")
                .append("- Experiments: ").append(results.size()).append("\n\n")
                .append("| Experiment | Family | Fault | Result | Cleanup |\n")
                .append("|---|---|---|---|---|\n");
        for (ExperimentResult result : results) {
            ExperimentPlan p = result.plan();
            Path exp = root.resolve("experiments").resolve(p.experimentId());
            Files.createDirectories(exp);
            ReplayCodec.write(exp.resolve("replay.json"), p);
            writeSamples(exp.resolve("samples.csv"), result.samples());
            Files.writeString(exp.resolve("result.json"), resultJson(result), StandardCharsets.UTF_8);
            summary.append('|').append(p.experimentId()).append('|').append(p.hypothesisFamily())
                    .append('|').append(p.faultType()).append('|').append(result.failureClass())
                    .append('|').append(result.cleanupConfirmed()).append("|\n");
        }
        Files.writeString(root.resolve("events.jsonl"), eventsJson(events), StandardCharsets.UTF_8);
        Files.writeString(root.resolve("summary.md"), summary, StandardCharsets.UTF_8);
        return root;
    }

    private static void writeSamples(Path path, List<ChaosLoadEngine.Sample> samples) throws IOException {
        StringBuilder csv = new StringBuilder("timestamp,elapsed,attempted,committed,failed,ambiguous,p95_ms,pool_state,ready,leased,total,heap_mb,urp,offline\n");
        for (ChaosLoadEngine.Sample s : samples) csv.append(s.timestamp()).append(',').append(s.elapsedSecond())
                .append(',').append(s.attempted()).append(',').append(s.committed()).append(',').append(s.failed())
                .append(',').append(s.ambiguous()).append(',').append(s.p95LatencyMs()).append(',').append(s.poolState())
                .append(',').append(s.readyProducers()).append(',').append(s.leasedProducers()).append(',')
                .append(s.totalProducers()).append(',').append(s.heapUsedMb()).append(',')
                .append(s.underReplicatedPartitions()).append(',').append(s.offlinePartitions()).append('\n');
        Files.writeString(path, csv, StandardCharsets.UTF_8);
    }

    private static String resultJson(ExperimentResult r) {
        return "{\"failureClass\":\"" + r.failureClass() + "\",\"cleanupConfirmed\":"
                + r.cleanupConfirmed() + ",\"detail\":\"" + escape(r.detail()) + "\",\"writtenAt\":\""
                + Instant.now() + "\"}\n";
    }
    private static String eventsJson(List<ChaosEvent> events) {
        StringBuilder out = new StringBuilder();
        for (ChaosEvent e : events) out.append("{\"timestamp\":\"").append(e.timestamp())
                .append("\",\"type\":\"").append(escape(e.type())).append("\",\"outcome\":\"")
                .append(escape(e.outcome())).append("\",\"detail\":\"").append(escape(e.detail())).append("\"}\n");
        return out.toString();
    }
    private static String escape(String s) { return s == null ? "" : s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n"); }
}
