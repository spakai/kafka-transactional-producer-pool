package com.kafka.producer.agentic;

import com.kafka.producer.chaos.VerificationReport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

class CampaignResultWriterTest {
    @TempDir Path temp;

    @Test void persistsNumericVerificationAndOffsetArtifacts() throws Exception {
        CampaignConfig config = CampaignConfig.fromArgs(new String[]{"--results-dir", temp.toString()});
        VerificationReport verification = new VerificationReport(
                4, 4, 40, 0, 0, 0, 0, 0, List.of(), List.of());
        ExperimentResult result = new ExperimentResult(TestFixtures.plan(FaultType.NONE),
                FailureClass.PASSED, "ok", true, List.of(), verification);

        Path campaign = new CampaignResultWriter().write(config, "campaign-test", "test",
                List.of(result), List.of());
        String json = Files.readString(campaign.resolve("experiments/exp-1/result.json"));
        String summary = Files.readString(campaign.resolve("summary.md"));
        assertTrue(json.contains("\"duplicatePublishIds\":0"));
        assertTrue(json.contains("\"crossTopicMismatches\":0"));
        assertTrue(Files.exists(campaign.resolve("experiments/exp-1/verification.csv")));
        assertTrue(Files.exists(campaign.resolve("experiments/exp-1/offsets.csv")));
        assertTrue(summary.contains("Duplicates"));
    }
}
