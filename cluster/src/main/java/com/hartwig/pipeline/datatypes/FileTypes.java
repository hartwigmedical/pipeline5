package com.hartwig.pipeline.datatypes;

public class FileTypes {

    public static final String BAM = "bam";
    public static final String VCF = "vcf";
    public static final String GZIPPED_VCF = VCF + ".gz";

    public static final String TBI = ".tbi";
    public static final String BAI = ".bai";
    public static final String CRAI = ".crai";
    public static final String CRAM = ".cram";

    public static String tabixIndex(final String vcf) {
        return vcf + TBI;
    }

    public static String bam(final String sample) {
        return sample + ".bam";
    }

    public static String cram(final String sample) {
        return sample + CRAM;
    }

    public static String crai(final String cram) {
        return cram + CRAI;
    }

    public static String bai(final String bam) {
        return bam + BAI;
    }
}
