package com.kafka.producer.chaos.scenarios;

import java.time.Duration;
import java.util.Map;

/**
 * CH-14: stalls one transaction after it has sent records for longer than the producer's
 * transaction.timeout.ms (default 6s for this scenario). The coordinator aborts the open
 * transaction; the later commit must fail, the producer must be evicted and rebuilt, and
 * none of the stalled transaction's records may be visible.
 */
public final class TransactionTimeoutScenario implements ChaosScenario {
    private boolean stalled;

    @Override
    public void onStart(ScenarioContext context) {
        if (context.config().transactionTimeoutMs() <= 0) {
            throw new IllegalStateException("CH-14 requires --transaction-timeout-ms > 0");
        }
        if (context.config().faultDuration().toMillis() <= context.config().transactionTimeoutMs()) {
            throw new IllegalStateException(
                    "CH-14 --fault-duration-sec (the stall) must exceed --transaction-timeout-ms");
        }
    }

    @Override
    public void onTick(ScenarioContext context, long elapsedSecond) {
        if (!stalled && elapsedSecond >= context.config().faultAt().toSeconds()) {
            Duration stall = context.config().faultDuration();
            context.load().stallNextTransaction(stall);
            stalled = true;
            context.event("transaction-stall", "requested", "stall beyond transaction.timeout.ms",
                    Map.of("stallMs", stall.toMillis(),
                            "transactionTimeoutMs", context.config().transactionTimeoutMs()));
        }
    }

    @Override
    public void onFinish(ScenarioContext context) throws Exception {
        ProducerPoisonScenario.awaitFullCapacity(context);
    }

    @Override
    public String evidenceGap(ScenarioContext context, boolean commitResponseProofConfirmed) {
        if (context.load().stallsConsumed() == 0) {
            return "No transaction was stalled";
        }
        return context.load().producerFencedTotal() > 0 ? null
                : "Stalled transaction did not produce a fencing failure (producer_fenced_total=0)";
    }
}
