package com.hartwig.pipeline.turquoise;

import java.util.Optional;

import org.immutables.value.Value;

@Value.Immutable
public interface PipelineProperties {
    String SAMPLE = "sample";
    String SET = "set";
    String TYPE = "type";
    String BARCODE = "barcode";
    String RUN_ID = "run_id";

    String sample();

    String set();

    Optional<Integer> runId();

    String type();

    Optional<String> referenceBarcode();

    Optional<String> tumorBarcode();

    static ImmutablePipelineProperties.Builder builder() {
        return ImmutablePipelineProperties.builder();
    }
}
