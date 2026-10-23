package com.kafka.producer.chaos;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.List;

public final class ChaosResultWriter {
    public ResultFiles write(
            ChaosConfig config,
            String runId,
            List<ChaosLoadEngine.Sample> samples,
            List<ChaosEvent> events,
            VerificationReport verification) throws IOException {
        return write(config, runId, samples, events, verification,
                verification.passed() ? "PASS" : "FAIL", "");
    }

    public ResultFiles write(
            ChaosConfig config,
            String runId,
            List<ChaosLoadEngine.Sample> samples,
            List<ChaosEvent> events,
            VerificationReport verification,
            String resultStatus,
            String resultDetail) throws IOException {
        Files.createDirectories(config.resultsDirectory());
        String stem = config.scenario() + "-" + timestamp();
        Path samplesFile = config.resultsDirectory().resolve(stem + ".csv");
        Path eventsFile = config.resultsDirectory().resolve(stem + "-events.jsonl");
        Path verificationFile = config.resultsDirectory().resolve(stem + "-verification.csv");
        Path offsetsFile = config.resultsDirectory().resolve(stem + "-offsets.csv");
        Path partitionsFile = config.resultsDirectory().resolve(stem + "-partition-counts.csv");

        writeSamples(samplesFile, samples);
        writeEvents(eventsFile, events);
        writeVerification(verificationFile, verification);
        writeOffsets(offsetsFile, verification);
        writePartitionCounts(partitionsFile, verification);
        appendSummary(config, runId, samplesFile, eventsFile, verificationFile, offsetsFile,
                partitionsFile, verification, resultStatus, resultDetail);
        return new ResultFiles(samplesFile, eventsFile, verificationFile, offsetsFile, partitionsFile);
    }

    private static void writeSamples(Path file, List<ChaosLoadEngine.Sample> samples)
            throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) {
            writer.write("timestamp,elapsed_second,attempted,committed,failed,aborted,ambiguous,"
                    + "p50_latency_ms,p95_latency_ms,p99_latency_ms,pool_state,"
                    + "ready_producers,leased_producers,total_producers,heap_used_mb,"
                    + "under_replicated_partitions,offline_partitions\n");
            for (ChaosLoadEngine.Sample sample : samples) {
                writer.write(String.format(java.util.Locale.ROOT,
                        "%s,%d,%d,%d,%d,%d,%d,%.3f,%.3f,%.3f,%s,%d,%d,%d,%.3f,%d,%d%n",
                        sample.timestamp(), sample.elapsedSecond(), sample.attempted(),
                        sample.committed(), sample.failed(), sample.aborted(), sample.ambiguous(),
                        sample.p50LatencyMs(), sample.p95LatencyMs(), sample.p99LatencyMs(),
                        sample.poolState(), sample.readyProducers(), sample.leasedProducers(),
                        sample.totalProducers(), sample.heapUsedMb(),
                        sample.underReplicatedPartitions(), sample.offlinePartitions()));
            }
        }
    }

    private void writeEvents(Path file, List<ChaosEvent> events) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) {
            for (ChaosEvent event : events) {
                writer.write(eventJson(event));
                writer.newLine();
            }
        }
    }

    private static String eventJson(ChaosEvent event) {
        StringBuilder attributes = new StringBuilder("{");
        boolean first = true;
        for (java.util.Map.Entry<String, Object> attribute : event.attributes().entrySet()) {
            if (!first) {
                attributes.append(',');
            }
            first = false;
            attributes.append(json(attribute.getKey())).append(':')
                    .append(json(String.valueOf(attribute.getValue())));
        }
        attributes.append('}');
        return "{\"timestamp\":" + json(event.timestamp().toString())
                + ",\"type\":" + json(event.type())
                + ",\"outcome\":" + json(event.outcome())
                + ",\"detail\":" + json(event.detail())
                + ",\"attributes\":" + attributes + "}";
    }

    private static String json(String value) {
        String escaped = value
                .replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
        return "\"" + escaped + "\"";
    }

    private static void writeVerification(Path file, VerificationReport verification)
            throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) {
            writer.write("publish_id,ledger_outcome,expected_records,observed_records,observed_uncommitted_records,"
                    + "callback_attempts,error_class\n");
            for (VerificationReport.VerificationRow row : verification.rows()) {
                writer.write(csv(row.publishId()) + "," + csv(row.ledgerOutcome()) + ","
                        + row.expectedRecords() + "," + row.observedRecords() + ","
                        + row.observedUncommittedRecords() + ","
                        + row.callbackAttempts() + "," + csv(row.errorClass()) + "\n");
            }
        }
    }

    private static String csv(String value) {
        return "\"" + value.replace("\"", "\"\"") + "\"";
    }

    private static void writeOffsets(Path file, VerificationReport report) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) {
            writer.write("topic,partition,start_offset,end_offset\n");
            for (var entry : report.offsetRanges().entrySet()) {
                writer.write(csv(entry.getKey().topic()) + "," + entry.getKey().partition() + ","
                        + entry.getValue().startOffset() + "," + entry.getValue().endOffset() + "\n");
            }
        }
    }

    private static void writePartitionCounts(Path file, VerificationReport report) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) {
            writer.write("topic,partition,read_committed_records\n");
            for (var e : report.committedRecordsByPartition().entrySet()) {
                writer.write(csv(e.getKey().topic()) + "," + e.getKey().partition() + "," + e.getValue() + "\n");
            }
        }
    }

    private static void appendSummary(
            ChaosConfig config,
            String runId,
            Path samples,
            Path events,
            Path verificationFile,
            Path offsetsFile,
            Path partitionsFile,
            VerificationReport report,
            String resultStatus,
            String resultDetail) throws IOException {
        Path summary = config.resultsDirectory().resolve("summary.md");
        String text = "\n## " + config.scenario() + " — " + Instant.now() + "\n\n"
                + "- Run ID: `" + runId + "`\n"
                + "- Result: **" + resultStatus + "**\n"
                + "- Result detail: " + resultDetail + "\n"
                + "- Ledger entries: " + report.ledgerEntries() + "\n"
                + "- Observed publish IDs: " + report.observedPublishIds() + "\n"
                + "- Missing committed publishes: " + report.missingCommitted() + "\n"
                + "- Visible conclusive failures: " + report.visibleFailed() + "\n"
                + "- Duplicate publish IDs: " + report.duplicatePublishIds() + "\n"
                + "- Partial transactions: " + report.partialTransactions() + "\n"
                + "- Ordering violations: " + report.orderingViolations() + "\n"
                + "- Aborted transactions: " + report.abortedTransactions() + "\n"
                + "- Aborted visible to read_committed: " + report.abortedVisibleCommitted() + "\n"
                + "- Aborted records visible to read_uncommitted: " + report.abortedVisibleUncommittedRecords() + "\n"
                + "- Aborted read_uncommitted mismatches: " + report.abortedUncommittedMismatches() + "\n"
                + "- Missing abort control samples: " + report.missingAbortControlSamples() + "\n"
                + "- Cross-topic mismatches: " + report.crossTopicMismatches() + "\n"
                + "- Ambiguous callback replays: " + report.callbackReplayViolations() + "\n"
                + "- Pending transactions after drain: " + report.pendingTransactions() + "\n"
                + "- Under-sampled MB-02 partitions: " + report.underSampledPartitions() + "\n"
                + "- Samples: `" + samples.getFileName() + "`\n"
                + "- Events: `" + events.getFileName() + "`\n"
                + "- Verification: `" + verificationFile.getFileName() + "`\n";
        text += "- Offset ranges: `" + offsetsFile.getFileName() + "`\n";
        text += "- Partition counts: `" + partitionsFile.getFileName() + "`\n";
        Files.writeString(summary, text, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    private static String timestamp() {
        return DateTimeFormatter.ISO_INSTANT.format(Instant.now())
                .replace(":", "-");
    }

    public record ResultFiles(Path samples, Path events, Path verification, Path offsets, Path partitionCounts) {}
}
