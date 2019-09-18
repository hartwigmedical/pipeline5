package com.hartwig.pipeline.metadata;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import org.immutables.value.Value;

@JsonSerialize(as = ImmutableSingleSampleRunMetadata.class)
@Value.Immutable
public interface SingleSampleRunMetadata {

    enum SampleType {
        TUMOR,
        REFERENCE
    }

    @Value.Default
    default String sampleName() {
        return sampleId();
    }

    @Value.Default
    @JsonIgnore
    default int entityId() {
        return -1;
    }

    String sampleId();

    SampleType type();

    static ImmutableSingleSampleRunMetadata.Builder builder() {
        return ImmutableSingleSampleRunMetadata.builder();
    }
}
