package com.kafka.producer.agentic;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CampaignConfigTest {
    @Test void requiresBothOptIns() {
        CampaignConfig config = CampaignConfig.fromArgs(new String[]{
                "--agentic-enabled", "true", "--cluster-allowlist", "x",
                "--environment-label", "disposable"});
        assertThrows(IllegalStateException.class, config::requireAuthorization);
    }

    @Test void acceptsExplicitDisposableAuthorization() {
        assertDoesNotThrow(TestFixtures.config()::requireAuthorization);
    }

    @Test void rejectsProductionLabel() {
        CampaignConfig config = CampaignConfig.fromArgs(new String[]{
                "--agentic-enabled", "true", "--chaos-enabled", "true",
                "--cluster-allowlist", "x", "--environment-label", "production"});
        assertThrows(IllegalStateException.class, config::requireAuthorization);
    }
}
