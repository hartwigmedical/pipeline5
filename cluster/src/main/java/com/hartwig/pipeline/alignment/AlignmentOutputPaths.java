package com.hartwig.pipeline.alignment;

public class AlignmentOutputPaths {

    public static String sorted(String sample) {
        return String.format("%s.sorted.bam", sample);
    }

    public static String bai(String bam) {
        return bam + ".bai";
    }

    public static String metrics(final String sample) {
        return String.format("%s.metrics", sample);
    }
}
