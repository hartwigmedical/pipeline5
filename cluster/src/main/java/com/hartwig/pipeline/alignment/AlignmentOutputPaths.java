package com.hartwig.pipeline.alignment;

import com.hartwig.patient.Sample;

public class AlignmentOutputPaths {

    public static String sorted(Sample sample) {
        return String.format("%s.sorted.bam", sample.name());
    }

    public static String bai(Sample sample) {
        return sorted(sample) + ".bai";
    }

    static String recalibrated(final Sample sample) {
        return String.format("%s.recalibrated.bam", sample.name());
    }

    public static String metrics(final Sample sample) {
        return String.format("%s.metrics", sample.name());
    }
}
