package com.hartwig.pipeline.testsupport;

import com.hartwig.pipeline.alignment.AlignmentOutput;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.alignment.vm.VmAligner;
import com.hartwig.pipeline.calling.germline.GermlineCallerOutput;
import com.hartwig.pipeline.calling.somatic.SomaticCaller;
import com.hartwig.pipeline.calling.somatic.SomaticCallerOutput;
import com.hartwig.pipeline.calling.structural.StructuralCaller;
import com.hartwig.pipeline.calling.structural.StructuralCallerOutput;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.flagstat.FlagstatOutput;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.metrics.BamMetrics;
import com.hartwig.pipeline.metrics.BamMetricsOutput;
import com.hartwig.pipeline.snpgenotype.SnpGenotypeOutput;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.tertiary.amber.Amber;
import com.hartwig.pipeline.tertiary.amber.AmberOutput;
import com.hartwig.pipeline.tertiary.cobalt.Cobalt;
import com.hartwig.pipeline.tertiary.cobalt.CobaltOutput;
import com.hartwig.pipeline.tertiary.healthcheck.HealthCheckOutput;
import com.hartwig.pipeline.tertiary.healthcheck.HealthChecker;
import com.hartwig.pipeline.tertiary.linx.LinxOutput;
import com.hartwig.pipeline.tertiary.purple.Purple;
import com.hartwig.pipeline.tertiary.purple.PurpleOutput;

import org.jetbrains.annotations.NotNull;

public class TestInputs {

    private static final String RESULTS = "results/";
    private static final String REFERENCE_SAMPLE = "reference";
    private static final String TUMOR_SAMPLE = "tumor";

    public static final String REFERENCE_BUCKET = "run-" + REFERENCE_SAMPLE + "-test";
    public static final String TUMOR_BUCKET = "run-" + TUMOR_SAMPLE + "-test";
    public static final String SOMATIC_BUCKET = "run-" + REFERENCE_SAMPLE + "-" + TUMOR_SAMPLE + "-test";

    public static String referenceSample() {
        return REFERENCE_SAMPLE;
    }

    public static String tumorSample() {
        return TUMOR_SAMPLE;
    }

    public static SomaticRunMetadata defaultSomaticRunMetadata() {
        final SingleSampleRunMetadata tumor = tumorRunMetadata();
        final SingleSampleRunMetadata reference = referenceRunMetadata();
        return SomaticRunMetadata.builder().runName("run").maybeTumor(tumor).reference(reference).build();
    }

    @NotNull
    public static SingleSampleRunMetadata referenceRunMetadata() {
        return SingleSampleRunMetadata.builder()
                .type(SingleSampleRunMetadata.SampleType.REFERENCE)
                .sampleId(referenceAlignmentOutput().sample())
                .build();
    }

    @NotNull
    public static SingleSampleRunMetadata tumorRunMetadata() {
        return SingleSampleRunMetadata.builder()
                .type(SingleSampleRunMetadata.SampleType.TUMOR)
                .sampleId(tumorAlignmentOutput().sample())
                .build();
    }

    public static AlignmentPair defaultPair() {
        return AlignmentPair.of(referenceAlignmentOutput(), tumorAlignmentOutput());
    }

    public static AlignmentOutput referenceAlignmentOutput() {
        return alignerOutput(REFERENCE_SAMPLE);
    }

    public static AlignmentOutput tumorAlignmentOutput() {
        return alignerOutput(TUMOR_SAMPLE);
    }

    private static AlignmentOutput alignerOutput(final String sample) {
        String bucket = namespacedBucket(sample, VmAligner.NAMESPACE);
        return AlignmentOutput.builder()
                .status(PipelineStatus.SUCCESS)
                .maybeFinalBamLocation(gsLocation(bucket, RESULTS + sample + ".bam"))
                .maybeFinalBaiLocation(gsLocation(bucket, RESULTS + sample + ".bam.bai"))
                .sample(sample)
                .build();
    }

    public static BamMetricsOutput referenceMetricsOutput() {
        return metricsOutput(REFERENCE_SAMPLE);
    }

    public static BamMetricsOutput tumorMetricsOutput() {
        return metricsOutput(TUMOR_SAMPLE);
    }

    public static SnpGenotypeOutput snpGenotypeOutput() {
        return SnpGenotypeOutput.builder().status(PipelineStatus.SUCCESS).build();
    }

    public static FlagstatOutput flagstatOutput() {
        return FlagstatOutput.builder().status(PipelineStatus.SUCCESS).build();
    }

    public static GermlineCallerOutput germlineCallerOutput() {
        return GermlineCallerOutput.builder().status(PipelineStatus.SUCCESS).build();
    }

    private static BamMetricsOutput metricsOutput(final String sample) {
        return BamMetricsOutput.builder()
                .status(PipelineStatus.SUCCESS)
                .sample(sample)
                .maybeMetricsOutputFile(gsLocation(namespacedBucket(sample, BamMetrics.NAMESPACE), BamMetricsOutput.outputFile(sample)))
                .build();
    }

    public static SomaticCallerOutput somaticCallerOutput() {
        return SomaticCallerOutput.builder()
                .status(PipelineStatus.SUCCESS)
                .maybeFinalSomaticVcf(gsLocation(somaticBucket(SomaticCaller.NAMESPACE),
                        RESULTS + TUMOR_SAMPLE + "." + OutputFile.GZIPPED_VCF))
                .build();
    }

    public static StructuralCallerOutput structuralCallerOutput() {
        String filtered = ".gridss.filtered.";
        String full = ".gridss.full.";
        return StructuralCallerOutput.builder()
                .status(PipelineStatus.SUCCESS)
                .maybeFilteredVcf(gsLocation(somaticBucket(StructuralCaller.NAMESPACE),
                        RESULTS + TUMOR_SAMPLE + filtered + OutputFile.GZIPPED_VCF))
                .maybeFilteredVcfIndex(gsLocation(somaticBucket(StructuralCaller.NAMESPACE),
                        RESULTS + TUMOR_SAMPLE + filtered + OutputFile.GZIPPED_VCF + ".tbi"))
                .maybeFullVcf(gsLocation(somaticBucket(StructuralCaller.NAMESPACE), RESULTS + TUMOR_SAMPLE + full + OutputFile.GZIPPED_VCF))
                .maybeFullVcfIndex(gsLocation(somaticBucket(StructuralCaller.NAMESPACE),
                        RESULTS + TUMOR_SAMPLE + full + OutputFile.GZIPPED_VCF + ".tbi"))
                .build();
    }

    private static String somaticBucket(String namespace) {
        return SOMATIC_BUCKET + "/" + namespace;
    }

    public static AmberOutput amberOutput() {
        return AmberOutput.builder()
                .status(PipelineStatus.SUCCESS)
                .maybeOutputDirectory(gsLocation(somaticBucket(Amber.NAMESPACE), RESULTS))
                .build();
    }

    public static CobaltOutput cobaltOutput() {
        return CobaltOutput.builder()
                .status(PipelineStatus.SUCCESS)
                .maybeOutputDirectory(gsLocation(somaticBucket(Cobalt.NAMESPACE), RESULTS))
                .build();
    }

    public static PurpleOutput purpleOutput() {
        return PurpleOutput.builder()
                .status(PipelineStatus.SUCCESS)
                .maybeOutputDirectory(gsLocation(somaticBucket(Purple.NAMESPACE), RESULTS))
                .build();
    }

    public static HealthCheckOutput healthCheckerOutput() {
        return HealthCheckOutput.builder()
                .status(PipelineStatus.SUCCESS)
                .maybeOutputDirectory(gsLocation(somaticBucket(HealthChecker.NAMESPACE), RESULTS))
                .build();
    }

    public static LinxOutput linxOutput() {
        return LinxOutput.builder()
                .status(PipelineStatus.SUCCESS)
                .build();
    }

    private static GoogleStorageLocation gsLocation(final String bucket, final String path) {
        return GoogleStorageLocation.of(bucket, path);
    }

    private static String namespacedBucket(final String sample, final String namespace) {
        return "run-" + sample + "-test/" + namespace;
    }
}
