package com.hartwig.pipeline.turquoise;

import java.util.Optional;

import org.immutables.value.Value;

@Value.Immutable
public interface PipelineSubjects {
    String SAMPLE = "sample";
    String SET = "set";
    String TYPE = "type";
    String BARCODE = "barcode";
    String RUN_ID = "run_id";

    String sample();

    String set();

    Optional<Integer> runId();

    String type();

    String referenceBarcode();

    Optional<String> tumorBarcode();

    static ImmutablePipelineSubjects.Builder builder() {
        return ImmutablePipelineSubjects.builder();
    }
}
