package com.kafka.producer.chaos;

import org.apache.kafka.common.TopicPartition;

import java.util.List;

public record VerificationReport(
        int ledgerEntries,
        int observedPublishIds,
        long observedRecords,
        int missingCommitted,
        int visibleFailed,
        int duplicatePublishIds,
        int partialTransactions,
        int orderingViolations,
        int abortedTransactions,
        int abortedVisibleCommitted,
        long abortedVisibleUncommittedRecords,
        int abortedUncommittedMismatches,
        int missingAbortControlSamples,
        int crossTopicMismatches,
        int callbackReplayViolations,
        int pendingTransactions,
        java.util.Map<TopicPartition, OffsetRange> offsetRanges,
        java.util.Map<TopicPartition, Long> committedRecordsByPartition,
        int underSampledPartitions,
        List<String> issues,
        List<VerificationRow> rows) {

    public VerificationReport(int ledgerEntries, int observedPublishIds, long observedRecords,
            int missingCommitted, int visibleFailed, int duplicatePublishIds,
            int partialTransactions, int orderingViolations, List<String> issues,
            List<VerificationRow> rows) {
        this(ledgerEntries, observedPublishIds, observedRecords, missingCommitted,
                visibleFailed, duplicatePublishIds, partialTransactions, orderingViolations,
                0, 0, 0, 0, 0, 0, 0, 0, java.util.Map.of(), java.util.Map.of(), 0, issues, rows);
    }

    public VerificationReport {
        issues = List.copyOf(issues);
        rows = List.copyOf(rows);
        offsetRanges = java.util.Map.copyOf(offsetRanges);
        committedRecordsByPartition = java.util.Map.copyOf(committedRecordsByPartition);
    }

    public boolean passed() {
        return missingCommitted == 0
                && visibleFailed == 0
                && duplicatePublishIds == 0
                && partialTransactions == 0
                && orderingViolations == 0
                && abortedVisibleCommitted == 0
                && abortedUncommittedMismatches == 0
                && missingAbortControlSamples == 0
                && crossTopicMismatches == 0
                && callbackReplayViolations == 0
                && pendingTransactions == 0
                && underSampledPartitions == 0;
    }

    public record VerificationRow(
            String publishId,
            String ledgerOutcome,
            int expectedRecords,
            int observedRecords,
            int observedUncommittedRecords,
            int callbackAttempts,
            String errorClass) {
        public VerificationRow(String publishId, String ledgerOutcome, int expectedRecords,
                int observedRecords, int callbackAttempts, String errorClass) {
            this(publishId, ledgerOutcome, expectedRecords, observedRecords, 0,
                    callbackAttempts, errorClass);
        }
    }

    public record OffsetRange(long startOffset, long endOffset) {}
}
