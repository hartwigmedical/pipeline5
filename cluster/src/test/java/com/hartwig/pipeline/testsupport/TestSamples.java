package com.hartwig.pipeline.testsupport;

import com.hartwig.patient.ImmutableSample;
import com.hartwig.patient.Sample;

public class TestSamples {

    public static Sample simpleReferenceSample() {
        return simpleSampleBuilder().type(Sample.Type.REFERENCE).build();
    }

    private static ImmutableSample.Builder simpleSampleBuilder() {
        return Sample.builder("directory", "sample");
    }

    public static Sample simpleTumorSample() {
        return simpleSampleBuilder().type(Sample.Type.TUMOR).build();
    }
}
