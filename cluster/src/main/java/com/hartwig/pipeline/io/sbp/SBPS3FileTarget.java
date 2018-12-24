package com.hartwig.pipeline.io.sbp;

import com.hartwig.patient.Sample;

public class SBPS3FileTarget {

    static final String ROOT_BUCKET = "hmf-bam-storage";

    public static String from(final Sample sample) {
        return String.format("s3://%s/%s/%s.bam", ROOT_BUCKET, sample.barcode(), sample.name());
    }
}
