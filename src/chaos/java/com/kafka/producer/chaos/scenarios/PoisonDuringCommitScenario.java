package com.kafka.producer.chaos.scenarios;

/**
 * CH-10: drops EndTxn responses (CH-05 proxy) and fences the pool's producers while
 * commits are unresolved. An ambiguous commit followed by eviction must never replay.
 */
public final class PoisonDuringCommitScenario implements ChaosScenario {
    private static final long TIMEOUT_GRACE_SECONDS = 2;

    private boolean armed;
    private boolean proofCaptured;
    private boolean fenced;
    private boolean healed;

    @Override
    public void onStart(ScenarioContext context) {
        if (context.config().commitResponseCommand().isBlank()
                || context.config().captureCommitResponseCommand().isBlank()
                || context.config().healNetworkCommand().isBlank()) {
            throw new IllegalStateException("CH-10 requires commit-response, capture-proof, and heal hooks");
        }
        long captureDelay = captureDelaySeconds(context);
        if (context.config().faultDuration().toSeconds() <= captureDelay) {
            throw new IllegalStateException("CH-10 fault duration must exceed request timeout plus "
                    + TIMEOUT_GRACE_SECONDS
                    + " seconds so broker state can be captured before fencing");
        }
        context.event("poison-during-commit", "scheduled", "EndTxn response drop plus fencing");
    }

    @Override
    public void onTick(ScenarioContext context, long elapsedSecond) throws Exception {
        long faultAt = context.config().faultAt().toSeconds();
        long captureAt = faultAt + captureDelaySeconds(context);
        long healAt = faultAt + context.config().faultDuration().toSeconds();
        if (!armed && elapsedSecond >= faultAt) {
            context.controller().partitionCommitResponse();
            armed = true;
        }
        if (armed && !proofCaptured && elapsedSecond >= captureAt) {
            context.controller().captureCommitResponseProof();
            proofCaptured = true;
        }
        if (armed && proofCaptured && !fenced) {
            for (int slot = 0; slot < context.load().poolSize(); slot++) {
                context.controller().fenceProducer(context.load().transactionalId(slot));
            }
            fenced = true;
        }
        if (armed && !healed && elapsedSecond >= healAt) {
            context.controller().healNetwork();
            healed = true;
        }
    }

    @Override
    public void onFinish(ScenarioContext context) throws Exception {
        if (armed && !healed) {
            context.controller().healNetwork();
            healed = true;
        }
        ProducerPoisonScenario.awaitFullCapacity(context);
    }

    @Override
    public String evidenceGap(ScenarioContext context, boolean commitResponseProofConfirmed) {
        if (!commitResponseProofConfirmed) {
            return "Missing correlated EndTxn response-drop and pre-fencing broker-state proof";
        }
        return fenced ? null : "Producers were never fenced";
    }

    private static long captureDelaySeconds(ScenarioContext context) {
        long timeoutSeconds = (context.config().requestTimeoutMs() + 999) / 1_000;
        return timeoutSeconds + TIMEOUT_GRACE_SECONDS;
    }
}
