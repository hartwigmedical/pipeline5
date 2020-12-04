package com.hartwig.pipeline.metadata;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import org.immutables.value.Value;

@JsonSerialize(as = ImmutableSingleSampleRunMetadata.class)
@Value.Immutable
public interface SingleSampleRunMetadata extends RunMetadata {

    enum SampleType {
        TUMOR,
        REFERENCE
    }

    @Value.Default
    default String sampleName() {
        return barcode();
    }

    @Value.Default
    @JsonIgnore
    default int entityId() {
        return -1;
    }

    String barcode();

    SampleType type();

    @Override
    default String name() {
        return barcode();
    }

    List<Integer> primaryTumorDoids();

    static ImmutableSingleSampleRunMetadata.Builder builder() {
        return ImmutableSingleSampleRunMetadata.builder();
    }
}
