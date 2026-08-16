package com.kafka.producer.agentic;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ReplayCodecTest {
    @TempDir Path temp;

    @Test void roundTripsResolvedPlan() throws Exception {
        ExperimentPlan expected = TestFixtures.plan(FaultType.STOP_LEADER_BROKER);
        Path replay = temp.resolve("replay.json");
        ReplayCodec.write(replay, expected);
        assertEquals(expected, ReplayCodec.read(replay));
    }
}
