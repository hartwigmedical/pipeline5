package com.hartwig.pipeline.input;

import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(as = ImmutablePipelineInput.class)
@Value.Style(jdkOnly = true)
public interface PipelineInput {

    Optional<ExternalIds> externalIds();

    Optional<Sample> reference();

    Optional<Sample> tumor();

    Map<String, Map<String, Map<String, String>>> dataset();

    default Sample sampleFor(final SingleSampleRunMetadata metadata) {
        return sample(metadata.type()).orElseThrow();
    }

    default Optional<Sample> sample(final SingleSampleRunMetadata.SampleType sampleType) {
        try {
            if (sampleType.equals(SingleSampleRunMetadata.SampleType.REFERENCE)) {
                return reference();
            } else {
                return tumor();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}