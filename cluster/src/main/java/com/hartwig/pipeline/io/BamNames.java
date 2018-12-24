package com.hartwig.pipeline.io;

import com.hartwig.patient.Sample;

public class BamNames {

    public static String sorted(Sample sample) {
        return String.format("%s.sorted.bam", sample.name());
    }

    public static String bai(Sample sample) {
        return sorted(sample) + ".bai";
    }
}
