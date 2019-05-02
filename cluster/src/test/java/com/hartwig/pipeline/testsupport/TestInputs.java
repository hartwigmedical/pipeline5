package com.hartwig.pipeline.testsupport;

import com.hartwig.patient.Sample;
import com.hartwig.pipeline.alignment.AlignmentOutput;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.io.GoogleStorageLocation;

import org.jetbrains.annotations.NotNull;

public class TestInputs {

    public static AlignmentPair defaultPair() {
        return AlignmentPair.of(alignerOutput("reference"), alignerOutput("tumor"));
    }

    private static AlignmentOutput alignerOutput(final String sample) {
        String bucket = "run-" + sample;
        return AlignmentOutput.of(gsLocation(bucket, sample + ".bam"),
                gsLocation(bucket, sample + ".bam.bai"),
                gsLocation(bucket, sample + ".recalibrated.bam"),
                gsLocation(bucket, sample + ".recalibrated.bam.bai"),
                Sample.builder("", sample).build());
    }

    @NotNull
    private static GoogleStorageLocation gsLocation(final String bucket, final String path) {
        return GoogleStorageLocation.of(bucket, path);
    }
}
