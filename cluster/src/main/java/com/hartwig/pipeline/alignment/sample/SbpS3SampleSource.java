package com.hartwig.pipeline.alignment.sample;

import com.hartwig.patient.Sample;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;

public class SbpS3SampleSource implements SampleSource {

    private final SbpSampleReader sbpSampleReader;

    public SbpS3SampleSource(final SbpSampleReader sbpSampleReader) {
        this.sbpSampleReader = sbpSampleReader;
    }

    @Override
    public SampleData sample(final SingleSampleRunMetadata metadata) {
        Sample sample = sbpSampleReader.read(metadata.entityId());
        return SampleData.of(sample, 0);
    }
}
