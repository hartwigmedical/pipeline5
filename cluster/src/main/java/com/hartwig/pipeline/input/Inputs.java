package com.hartwig.pipeline.input;

import java.util.Optional;

import com.hartwig.pdl.PipelineInput;
import com.hartwig.pdl.SampleInput;

public class Inputs {

    public static SampleInput sampleFor(final PipelineInput pipelineInput, final SingleSampleRunMetadata metadata) {
        return sample(pipelineInput, metadata.type()).orElseThrow();
    }

    public static Optional<SampleInput> sample(final PipelineInput pipelineInput, final SingleSampleRunMetadata.SampleType sampleType) {
        try {
            if (sampleType.equals(SingleSampleRunMetadata.SampleType.REFERENCE)) {
                return pipelineInput.reference();
            } else {
                return pipelineInput.tumor();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
