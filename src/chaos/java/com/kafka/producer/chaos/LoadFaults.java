package com.kafka.producer.chaos;

import java.time.Duration;

/** Fault and observation hooks into the running load engine and its producer pool. */
public interface LoadFaults {
    int poolSize();

    String transactionalId(int slot);

    /** Makes exactly one transaction callback sleep after sending, once. */
    void stallNextTransaction(Duration duration);

    long stallsConsumed();

    /** Shuts the pool down and starts a new one that reuses the same transactional IDs. */
    void restartPool();

    long poolRestarts();

    long shutdownRejections();

    long producerFencedTotal();

    /** True when the pool is HEALTHY and every slot is ready or leased. */
    boolean atFullCapacity();

    String poolState();
}
