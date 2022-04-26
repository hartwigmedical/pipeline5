package com.hartwig.pipeline.datatypes;

public class FileTypes {

    public static final String BAM = "bam";
    public static final String CRAM = "cram";
    public static final String VCF = "vcf";
    public static final String GZIPPED_VCF = VCF + ".gz";
    public static final String TSV = "tsv";

    public static final String TBI = ".tbi";
    public static final String BAI = ".bai";
    public static final String CRAI = ".crai";

    public static String tabixIndex(final String vcf) {
        return vcf + TBI;
    }

    public static String bam(final String sample) {
        return sample + "." + BAM;
    }

    public static String cram(final String sample) {
        return sample + "." + CRAM;
    }

    public static String crai(final String cram) {
        return cram + CRAI;
    }

    public static String bai(final String bam) {
        return bam + BAI;
    }

    public static boolean isBam(final String filename) {
        return filename.endsWith(BAM);
    }

    public static boolean isBai(final String filename) {
        return filename.endsWith(BAI);
    }

    public static boolean isCram(final String filename) {
        return filename.endsWith(CRAM);
    }

    public static boolean isCrai(final String filename) {
        return filename.endsWith(CRAI);
    }

    public static boolean isVcf(final String filename) {
        return filename.endsWith(VCF);
    }

    public static String toAlignmentIndex(final String filename) {
        if (isBam(filename)) {
            return bai(filename);
        } else if (isCram(filename)) {
            return crai(filename);
        }
        return filename;
    }
}
