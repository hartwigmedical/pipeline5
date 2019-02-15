package com.hartwig.pipeline.io.sources;

import com.hartwig.patient.Sample;

import org.immutables.value.Value;

@Value.Immutable
public interface SampleData {

    @Value.Parameter
    Sample sample();

    @Value.Parameter
    long sizeInBytesGZipped();

    static SampleData of(Sample sample, long size) {
        return ImmutableSampleData.of(sample, size);
    }
}
