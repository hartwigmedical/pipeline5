package com.hartwig.pipeline.metadata;

import org.immutables.value.Value;

@Value.Immutable
public interface SingleSampleRunMetadata {

    enum SampleType{
        TUMOR, REFERENCE
    }

    @Value.Default
    default String sampleName(){
        return sampleId();
    }

    String sampleId();

    SampleType type();

    static ImmutableSingleSampleRunMetadata.Builder builder(){
        return ImmutableSingleSampleRunMetadata.builder();
    }
}
