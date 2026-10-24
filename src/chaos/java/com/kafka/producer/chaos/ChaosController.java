package com.kafka.producer.chaos;

import java.time.Duration;

public interface ChaosController extends AutoCloseable {
    void stopBroker(int brokerId) throws Exception;

    void startBroker(int brokerId) throws Exception;

    void waitForBrokerReady(int brokerId, Duration timeout) throws Exception;

    void partitionProducerFromBroker(int brokerId) throws Exception;

    void partitionProducerFromCluster() throws Exception;

    /**
     * Enables a proxy or fault hook that drops the response around commit.
     * The command is environment-specific because Kafka commit timing cannot
     * be controlled safely by stopping a broker alone.
     */
    void partitionCommitResponse() throws Exception;

    /** Capture the proxy and broker transaction state before CH-10 fences its producers. */
    void captureCommitResponseProof() throws Exception;

    /**
     * Poisons a pool producer from outside the pool by running
     * {@code initTransactions} with the same transactional ID, which bumps the
     * epoch and fences the pool's instance.
     */
    void fenceProducer(String transactionalId) throws Exception;

    void healNetwork() throws Exception;

    void verifyCleanup() throws Exception;

    @Override
    void close() throws Exception;
}
