package com.hartwig.pipeline.testsupport;

import com.hartwig.pipeline.alignment.AlignmentOutput;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.metadata.ImmutableSingleSampleRunMetadata;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.storage.GoogleStorageLocation;

import org.jetbrains.annotations.NotNull;

public class TestInputs {

    private static final String RESULTS = "results/";

    public static SomaticRunMetadata defaultSomaticRunMetadata() {
        final SingleSampleRunMetadata tumor = tumorRunMetadata();
        final SingleSampleRunMetadata reference = referenceRunMetadata();
        return SomaticRunMetadata.builder().runName("run").maybeTumor(tumor).reference(reference).build();
    }

    public static SomaticRunMetadata defaultSingleSampleRunMetadata() {
        final SingleSampleRunMetadata reference = referenceRunMetadata();
        return SomaticRunMetadata.builder().runName("run").reference(reference).build();
    }

    @NotNull
    public static ImmutableSingleSampleRunMetadata referenceRunMetadata() {
        return SingleSampleRunMetadata.builder()
                .type(SingleSampleRunMetadata.SampleType.REFERENCE)
                .sampleId(referenceAlignmentOutput().sample())
                .build();
    }

    @NotNull
    private static ImmutableSingleSampleRunMetadata tumorRunMetadata() {
        return SingleSampleRunMetadata.builder()
                .type(SingleSampleRunMetadata.SampleType.TUMOR)
                .sampleId(tumorAlignmentOutput().sample())
                .build();
    }

    public static AlignmentPair defaultPair() {
        return AlignmentPair.of(referenceAlignmentOutput(), tumorAlignmentOutput());
    }

    public static AlignmentOutput referenceAlignmentOutput() {
        return alignerOutput("reference");
    }

    public static AlignmentOutput tumorAlignmentOutput() {
        return alignerOutput("tumor");
    }

    private static AlignmentOutput alignerOutput(final String sample) {
        String bucket = "run-" + sample + "/aligner";
        return AlignmentOutput.builder()
                .status(PipelineStatus.SUCCESS)
                .maybeFinalBamLocation(gsLocation(bucket, RESULTS + sample + ".bam"))
                .maybeFinalBaiLocation(gsLocation(bucket, RESULTS + sample + ".bam.bai"))
                .sample(sample)
                .build();
    }

    @NotNull
    private static GoogleStorageLocation gsLocation(final String bucket, final String path) {
        return GoogleStorageLocation.of(bucket, path);
    }
}