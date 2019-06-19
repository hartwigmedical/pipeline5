package com.hartwig.pipeline.metadata;

import org.immutables.value.Value;

@Value.Immutable
public interface SampleMetadata {

    String barcodeOrSampleName();

    String setName();

    static ImmutableSampleMetadata.Builder builder(){
        return ImmutableSampleMetadata.builder();
    }
}
