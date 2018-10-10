package com.hartwig.pipeline.upload;

import java.util.function.Function;

import com.hartwig.patient.Sample;

public class SBPS3FileTarget implements Function<Sample, String> {

    static final String ROOT_BUCKET = "hmf-bam-storage";

    @Override
    public String apply(final Sample sample) {
        return String.format("s3://%s/%s/%s.bam", ROOT_BUCKET, sample.barcode(), sample.name());
    }
}
