package com.kafka.producer.chaos;

import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public final class PublishLedger {
    public enum Outcome {
        ATTEMPTED,
        COMMITTED,
        ABORTED,
        FAILED,
        AMBIGUOUS
    }

    private final ConcurrentHashMap<String, Entry> entries = new ConcurrentHashMap<>();

    public Entry attempted(String publishId, String key, long sequence, int expectedRecords) {
        Entry entry = new Entry(publishId, key, sequence, expectedRecords, Instant.now());
        Entry existing = entries.putIfAbsent(publishId, entry);
        if (existing != null) {
            throw new IllegalStateException("Duplicate publish ID generated: " + publishId);
        }
        return entry;
    }

    public Collection<Entry> entries() {
        return Map.copyOf(entries).values();
    }

    public static final class Entry {
        private final String publishId;
        private final String key;
        private final long sequence;
        private final int expectedRecords;
        private final Instant attemptedAt;
        private final AtomicInteger callbackAttempts = new AtomicInteger();
        private volatile Outcome outcome = Outcome.ATTEMPTED;
        private volatile String errorClass = "";

        private Entry(String publishId, String key, long sequence, int expectedRecords, Instant attemptedAt) {
            this.publishId = publishId;
            this.key = key;
            this.sequence = sequence;
            this.expectedRecords = expectedRecords;
            this.attemptedAt = attemptedAt;
        }

        public void recordCallbackAttempt() {
            callbackAttempts.incrementAndGet();
        }

        public void complete(Outcome outcome, Throwable error) {
            this.outcome = outcome;
            this.errorClass = error == null ? "" : rootCause(error).getClass().getName();
        }

        private static Throwable rootCause(Throwable error) {
            Throwable current = error;
            while (current.getCause() != null && current.getCause() != current) {
                current = current.getCause();
            }
            return current;
        }

        public String publishId() { return publishId; }
        public String key() { return key; }
        public long sequence() { return sequence; }
        public int expectedRecords() { return expectedRecords; }
        public Instant attemptedAt() { return attemptedAt; }
        public int callbackAttempts() { return callbackAttempts.get(); }
        public Outcome outcome() { return outcome; }
        public String errorClass() { return errorClass; }
    }
}
