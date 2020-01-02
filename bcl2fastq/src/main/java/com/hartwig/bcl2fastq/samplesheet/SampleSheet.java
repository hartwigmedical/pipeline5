package com.hartwig.bcl2fastq.samplesheet;

import java.util.Set;

import org.immutables.value.Value;

@Value.Immutable
public interface SampleSheet {

    String experimentName();

    Set<String> projects();

    static ImmutableSampleSheet.Builder builder() {
        return ImmutableSampleSheet.builder();
    }
}
