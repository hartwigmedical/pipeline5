package com.hartwig.pipeline.testsupport;

import com.hartwig.patient.Sample;
import com.hartwig.pipeline.alignment.AlignmentOutput;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.execution.JobStatus;
import com.hartwig.pipeline.io.GoogleStorageLocation;

import org.jetbrains.annotations.NotNull;

public class TestInputs {

    private static final String RESULTS = "results/";

    public static AlignmentPair defaultPair() {
        return AlignmentPair.of(alignerOutput("reference", Sample.Type.REFERENCE), alignerOutput("tumor", Sample.Type.TUMOR));
    }

    private static AlignmentOutput alignerOutput(final String sample, final Sample.Type type) {
        String bucket = "run-" + sample + "/aligner";
        return AlignmentOutput.builder()
                .status(JobStatus.SUCCESS)
                .maybeFinalBamLocation(gsLocation(bucket, RESULTS + sample + ".bam"))
                .maybeFinalBaiLocation(gsLocation(bucket, RESULTS + sample + ".bam.bai"))
                .sample(Sample.builder("", sample).type(type).build())
                .build();
    }

    @NotNull
    private static GoogleStorageLocation gsLocation(final String bucket, final String path) {
        return GoogleStorageLocation.of(bucket, path);
    }
}
