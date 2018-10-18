package com.hartwig.pipeline.after;

import com.hartwig.patient.Sample;

class Bams {

    static final String UNSORTED = "unsorted";
    static final String SORTED = "sorted";
    private static final String BAI = ".bai";

    public static String name(final Sample sample, String directory, final String type) {
        return String.format("%s/%s.%s.bam", directory, sample.name(), type);
    }

    public static String bai(final Sample sample, String directory, final String type) {
        return name(sample, directory, type) + BAI;
    }
}
