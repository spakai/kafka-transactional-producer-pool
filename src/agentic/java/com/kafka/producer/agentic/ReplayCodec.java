package com.kafka.producer.agentic;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class ReplayCodec {
    private ReplayCodec() {}

    public static void write(Path path, ExperimentPlan p) throws IOException {
        Files.createDirectories(path.getParent());
        String json = """
                {
                  "experimentId":"%s",
                  "hypothesis":"%s",
                  "hypothesisFamily":"%s",
                  "faultType":"%s",
                  "brokerId":%d,
                  "targetPartition":%d,
                  "startAfterSeconds":%d,
                  "faultDurationSeconds":%d,
                  "threads":%d,
                  "recordsPerTransaction":%d,
                  "observeForSeconds":%d,
                  "expectedSignals":"%s"
                }
                """.formatted(escape(p.experimentId()), escape(p.hypothesis()), escape(p.hypothesisFamily()),
                p.faultType(), p.brokerId(), p.targetPartition(), p.startAfter().toSeconds(),
                p.faultDuration().toSeconds(), p.threads(), p.recordsPerTransaction(), p.observeFor().toSeconds(),
                escape(String.join(",", p.expectedSignals())));
        Files.writeString(path, json, StandardCharsets.UTF_8);
    }

    public static ExperimentPlan read(Path path) throws IOException {
        String json = Files.readString(path, StandardCharsets.UTF_8);
        return new ExperimentPlan(string(json, "experimentId"), string(json, "hypothesis"),
                string(json, "hypothesisFamily"), FaultType.valueOf(string(json, "faultType")),
                integer(json, "brokerId"), integer(json, "targetPartition"),
                Duration.ofSeconds(integer(json, "startAfterSeconds")),
                Duration.ofSeconds(integer(json, "faultDurationSeconds")), integer(json, "threads"),
                integer(json, "recordsPerTransaction"), Duration.ofSeconds(integer(json, "observeForSeconds")),
                List.of(string(json, "expectedSignals").split(",")));
    }

    private static String string(String json, String name) {
        Matcher m = Pattern.compile("\\\"" + name + "\\\"\\s*:\\s*\\\"((?:\\\\.|[^\\\"])*)\\\"").matcher(json);
        if (!m.find()) throw new IllegalArgumentException("Missing replay field: " + name);
        return m.group(1).replace("\\\"", "\"").replace("\\\\", "\\");
    }
    private static int integer(String json, String name) {
        Matcher m = Pattern.compile("\\\"" + name + "\\\"\\s*:\\s*(-?\\d+)").matcher(json);
        if (!m.find()) throw new IllegalArgumentException("Missing replay field: " + name);
        return Integer.parseInt(m.group(1));
    }
    private static String escape(String s) { return s.replace("\\", "\\\\").replace("\"", "\\\""); }
}
