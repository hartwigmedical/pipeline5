package com.hartwig.pipeline.alignment;

import static java.lang.String.format;

public class AlignmentOutputPaths {

    public static String bam(String name){
        return format("%s.bam", name);
    }

    public static String sorted(String sample) {
        return bam(format("%s.sorted", sample));
    }

    public static String bai(String bam) {
        return bam + ".bai";
    }

    public static String metrics(final String sample) {
        return format("%s.metrics", sample);
    }
}
