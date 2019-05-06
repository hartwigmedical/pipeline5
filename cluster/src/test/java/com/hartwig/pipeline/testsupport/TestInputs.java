package com.hartwig.pipeline.testsupport;

import com.hartwig.patient.Sample;
import com.hartwig.pipeline.alignment.AlignmentOutput;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.io.GoogleStorageLocation;

import org.jetbrains.annotations.NotNull;

public class TestInputs {

    private static final String RESULTS = "results/";

    public static AlignmentPair defaultPair() {
        return AlignmentPair.of(alignerOutput("reference"), alignerOutput("tumor"));
    }

    private static AlignmentOutput alignerOutput(final String sample) {
        String bucket = "run-" + sample + "/aligner";
        return AlignmentOutput.of(gsLocation(bucket, RESULTS + sample + ".bam"),
                gsLocation(bucket, RESULTS + sample + ".bam.bai"),
                gsLocation(bucket, RESULTS + sample + ".recalibrated.bam"),
                gsLocation(bucket, RESULTS + sample + ".recalibrated.bam.bai"),
                Sample.builder("", sample).build());
    }

    @NotNull
    private static GoogleStorageLocation gsLocation(final String bucket, final String path) {
        return GoogleStorageLocation.of(bucket, path);
    }
}
