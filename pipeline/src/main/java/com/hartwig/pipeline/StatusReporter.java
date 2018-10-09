package com.hartwig.pipeline;

public interface StatusReporter {
    String SUCCESS = "_SUCCESS";
    String FAILURE = "_FAILURE";

    void report(final StatusReporter.Status status);

    enum Status {
        SUCCESS,
        FAILED_READ_COUNT,
        FAILED_FINAL_QC,
        FAILED_ERROR
    }
}
