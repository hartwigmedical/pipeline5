package com.hartwig.pipeline.testsupport;

import java.util.List;

import com.hartwig.pipeline.alignment.Aligner;
import com.hartwig.pipeline.alignment.AlignmentOutput;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.calling.germline.GermlineCaller;
import com.hartwig.pipeline.calling.germline.GermlineCallerOutput;
import com.hartwig.pipeline.calling.sage.SageGermlineCaller;
import com.hartwig.pipeline.calling.sage.SageOutput;
import com.hartwig.pipeline.calling.sage.SageSomaticCaller;
import com.hartwig.pipeline.calling.structural.StructuralCaller;
import com.hartwig.pipeline.calling.structural.StructuralCallerOutput;
import com.hartwig.pipeline.calling.structural.StructuralCallerPostProcess;
import com.hartwig.pipeline.calling.structural.StructuralCallerPostProcessOutput;
import com.hartwig.pipeline.cram.CramOutput;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.flagstat.Flagstat;
import com.hartwig.pipeline.flagstat.FlagstatOutput;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.metrics.BamMetrics;
import com.hartwig.pipeline.metrics.BamMetricsOutput;
import com.hartwig.pipeline.resource.RefGenome37ResourceFiles;
import com.hartwig.pipeline.resource.RefGenome38ResourceFiles;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.snpgenotype.SnpGenotypeOutput;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.tertiary.amber.Amber;
import com.hartwig.pipeline.tertiary.amber.AmberOutput;
import com.hartwig.pipeline.tertiary.bachelor.Bachelor;
import com.hartwig.pipeline.tertiary.bachelor.BachelorOutput;
import com.hartwig.pipeline.tertiary.chord.Chord;
import com.hartwig.pipeline.tertiary.chord.ChordOutput;
import com.hartwig.pipeline.tertiary.cobalt.Cobalt;
import com.hartwig.pipeline.tertiary.cobalt.CobaltOutput;
import com.hartwig.pipeline.tertiary.cuppa.CuppaOutput;
import com.hartwig.pipeline.tertiary.healthcheck.HealthCheckOutput;
import com.hartwig.pipeline.tertiary.healthcheck.HealthChecker;
import com.hartwig.pipeline.tertiary.linx.Linx;
import com.hartwig.pipeline.tertiary.linx.LinxOutput;
import com.hartwig.pipeline.tertiary.linx.LinxOutputLocations;
import com.hartwig.pipeline.tertiary.peach.PeachOutput;
import com.hartwig.pipeline.tertiary.protect.ProtectOutput;
import com.hartwig.pipeline.tertiary.purple.Purple;
import com.hartwig.pipeline.tertiary.purple.PurpleOutput;
import com.hartwig.pipeline.tertiary.purple.PurpleOutputLocations;
import com.hartwig.pipeline.tertiary.virusbreakend.VirusBreakendOutput;

import org.jetbrains.annotations.NotNull;

public class TestInputs {

    private static final String RESULTS = "results/";
    private static final String REFERENCE_SAMPLE = "reference";
    private static final String TUMOR_SAMPLE = "tumor";

    public static final String REFERENCE_BUCKET = "run-" + REFERENCE_SAMPLE + "-test";
    public static final String TUMOR_BUCKET = "run-" + TUMOR_SAMPLE + "-test";
    public static final String SOMATIC_BUCKET = "run-" + REFERENCE_SAMPLE + "-" + TUMOR_SAMPLE + "-test";

    public static final ResourceFiles REF_GENOME_37_RESOURCE_FILES = new RefGenome37ResourceFiles();
    public static final ResourceFiles REF_GENOME_38_RESOURCE_FILES = new RefGenome38ResourceFiles();
    public static final String SET = "set";
    public static final String ID = "id";
    public static final String BUCKET = "bucket";

    public static String referenceSample() {
        return REFERENCE_SAMPLE;
    }

    public static String tumorSample() {
        return TUMOR_SAMPLE;
    }

    public static SomaticRunMetadata defaultSomaticRunMetadata() {
        final SingleSampleRunMetadata tumor = tumorRunMetadata();
        final SingleSampleRunMetadata reference = referenceRunMetadata();
        return SomaticRunMetadata.builder().set(SET).maybeTumor(tumor).reference(reference).bucket(BUCKET).build();
    }

    public static SomaticRunMetadata defaultSingleSampleRunMetadata() {
        final SingleSampleRunMetadata reference = referenceRunMetadata();
        return SomaticRunMetadata.builder().set(SET).reference(reference).bucket(BUCKET).build();
    }

    @NotNull
    public static SingleSampleRunMetadata referenceRunMetadata() {
        return SingleSampleRunMetadata.builder()
                .set(SET)
                .bucket(BUCKET)
                .type(SingleSampleRunMetadata.SampleType.REFERENCE)
                .barcode(referenceAlignmentOutput().sample())
                .build();
    }

    @NotNull
    public static SingleSampleRunMetadata tumorRunMetadata() {
        return SingleSampleRunMetadata.builder()
                .set(SET)
                .bucket(BUCKET)
                .type(SingleSampleRunMetadata.SampleType.TUMOR)
                .barcode(tumorAlignmentOutput().sample())
                .primaryTumorDoids(List.of("01", "02"))
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
        String bucket = namespacedBucket(sample, Aligner.NAMESPACE);
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
        return referenceFlagstatOutput();
    }

    public static FlagstatOutput flagstatOutput(final String sample) {
        return FlagstatOutput.builder()
                .status(PipelineStatus.SUCCESS)
                .sample(sample)
                .maybeFlagstatOutputFile(gsLocation(namespacedBucket(sample, Flagstat.NAMESPACE), sample + ".flagstat"))
                .build();
    }

    public static FlagstatOutput referenceFlagstatOutput() {
        return flagstatOutput(REFERENCE_SAMPLE);
    }

    public static FlagstatOutput tumorFlagstatOutput() {
        return flagstatOutput(TUMOR_SAMPLE);
    }

    public static CramOutput cramOutput() {
        return CramOutput.builder().status(PipelineStatus.SUCCESS).build();
    }

    public static GermlineCallerOutput germlineCallerOutput() {
        String germlineVcf = REFERENCE_SAMPLE + ".germline.vcf.gz";
        return GermlineCallerOutput.builder()
                .status(PipelineStatus.SUCCESS)
                .maybeGermlineVcfLocation(gsLocation(namespacedBucket(REFERENCE_SAMPLE, GermlineCaller.NAMESPACE), germlineVcf))
                .maybeGermlineVcfIndexLocation(gsLocation(namespacedBucket(REFERENCE_SAMPLE, GermlineCaller.NAMESPACE),
                        germlineVcf + ".tbi"))
                .build();
    }

    private static BamMetricsOutput metricsOutput(final String sample) {
        return BamMetricsOutput.builder()
                .status(PipelineStatus.SUCCESS)
                .sample(sample)
                .maybeMetricsOutputFile(gsLocation(namespacedBucket(sample, BamMetrics.NAMESPACE),
                        RESULTS + BamMetricsOutput.outputFile(sample)))
                .build();
    }

    public static VirusBreakendOutput virusBreakendOutput() {
        return VirusBreakendOutput.builder().status(PipelineStatus.SUCCESS).build();
    }

    public static SageOutput sageGermlineOutput() {
        return SageOutput.builder(SageGermlineCaller.NAMESPACE)
                .status(PipelineStatus.SUCCESS)
                .maybeFinalVcf(gsLocation(somaticBucket(SageGermlineCaller.NAMESPACE),
                        RESULTS + TUMOR_SAMPLE + ".germline." + FileTypes.GZIPPED_VCF))
                .build();
    }

    public static SageOutput sageSomaticOutput() {
        return SageOutput.builder(SageSomaticCaller.NAMESPACE)
                .status(PipelineStatus.SUCCESS)
                .maybeFinalVcf(gsLocation(somaticBucket(SageSomaticCaller.NAMESPACE),
                        RESULTS + TUMOR_SAMPLE + ".somatic." + FileTypes.GZIPPED_VCF))
                .build();
    }

    public static StructuralCallerOutput structuralCallerOutput() {
        String unfiltered = ".gridss.unfiltered.";
        return StructuralCallerOutput.builder()
                .status(PipelineStatus.SUCCESS)
                .maybeUnfilteredVcf(gsLocation(somaticBucket(StructuralCaller.NAMESPACE),
                        RESULTS + TUMOR_SAMPLE + unfiltered + FileTypes.GZIPPED_VCF))
                .maybeUnfilteredVcfIndex(gsLocation(somaticBucket(StructuralCaller.NAMESPACE),
                        RESULTS + TUMOR_SAMPLE + unfiltered + FileTypes.GZIPPED_VCF + ".tbi"))
                .build();
    }

    public static StructuralCallerPostProcessOutput structuralCallerPostProcessOutput() {
        String filtered = ".gripss.filtered.";
        String full = ".gripss.full.";
        return StructuralCallerPostProcessOutput.builder()
                .status(PipelineStatus.SUCCESS)
                .maybeFilteredVcf(gsLocation(somaticBucket(StructuralCallerPostProcess.NAMESPACE),
                        RESULTS + TUMOR_SAMPLE + filtered + FileTypes.GZIPPED_VCF))
                .maybeFilteredVcfIndex(gsLocation(somaticBucket(StructuralCallerPostProcess.NAMESPACE),
                        RESULTS + TUMOR_SAMPLE + filtered + FileTypes.GZIPPED_VCF + ".tbi"))
                .maybeFullVcf(gsLocation(somaticBucket(StructuralCallerPostProcess.NAMESPACE),
                        RESULTS + TUMOR_SAMPLE + full + FileTypes.GZIPPED_VCF))
                .maybeFullVcfIndex(gsLocation(somaticBucket(StructuralCallerPostProcess.NAMESPACE),
                        RESULTS + TUMOR_SAMPLE + full + FileTypes.GZIPPED_VCF + ".tbi"))
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
                .maybeOutputLocations(PurpleOutputLocations.builder()
                        .outputDirectory(gsLocation(somaticBucket(Purple.NAMESPACE), RESULTS))
                        .somaticVcf(gsLocation(somaticBucket(Purple.NAMESPACE), TUMOR_SAMPLE + Purple.PURPLE_SOMATIC_VCF))
                        .germlineVcf(gsLocation(somaticBucket(Purple.NAMESPACE), TUMOR_SAMPLE + Purple.PURPLE_GERMLINE_VCF))
                        .structuralVcf(gsLocation(somaticBucket(Purple.NAMESPACE), TUMOR_SAMPLE + Purple.PURPLE_SV_VCF))
                        .purityTsv(gsLocation(somaticBucket(Purple.NAMESPACE), TUMOR_SAMPLE + Purple.PURPLE_PURITY_TSV))
                        .qcFile(gsLocation(somaticBucket(Purple.NAMESPACE), TUMOR_SAMPLE + Purple.PURPLE_QC))
                        .somaticDriverCatalog(gsLocation(somaticBucket(Purple.NAMESPACE),
                                TUMOR_SAMPLE + Purple.PURPLE_SOMATIC_DRIVER_CATALOG))
                        .germlineDriverCatalog(gsLocation(somaticBucket(Purple.NAMESPACE),
                                TUMOR_SAMPLE + Purple.PURPLE_GERMLINE_DRIVER_CATALOG))
                        .build()).build();
    }

    public static ChordOutput chordOutput() {
        return ChordOutput.builder()
                .status(PipelineStatus.SUCCESS)
                .maybePredictions(gsLocation(somaticBucket(Chord.NAMESPACE), TUMOR_SAMPLE + Chord.PREDICTION_TXT))
                .build();
    }

    public static CuppaOutput cuppaOutput() {
        return CuppaOutput.builder().status(PipelineStatus.SUCCESS).build();
    }

    public static HealthCheckOutput healthCheckerOutput() {
        return HealthCheckOutput.builder()
                .status(PipelineStatus.SUCCESS)
                .maybeOutputDirectory(gsLocation(somaticBucket(HealthChecker.NAMESPACE), RESULTS))
                .build();
    }

    public static String namespacedBucket(final String sample, final String namespace) {
        return "run-" + sample + "-test/" + namespace;
    }

    public static LinxOutput linxOutput() {
        return LinxOutput.builder()
                .status(PipelineStatus.SUCCESS)
                .maybeLinxOutputLocations(LinxOutputLocations.builder()
                        .breakends(gsLocation(somaticBucket(Linx.NAMESPACE), TUMOR_SAMPLE + Linx.BREAKEND_TSV))
                        .driverCatalog(gsLocation(somaticBucket(Linx.NAMESPACE), TUMOR_SAMPLE + Linx.DRIVER_CATALOG_TSV))
                        .fusions(gsLocation(somaticBucket(Linx.NAMESPACE), TUMOR_SAMPLE + Linx.FUSION_TSV))
                        .build())
                .build();
    }

    public static BachelorOutput bachelorOutput() {
        return BachelorOutput.builder()
                .status(PipelineStatus.SUCCESS)
                .maybeReportableVariants(gsLocation(somaticBucket(Bachelor.NAMESPACE), TUMOR_SAMPLE + Bachelor.REPORTABLE_VARIANT_TSV))
                .build();
    }

    public static ProtectOutput protectOutput() {
        return ProtectOutput.builder().status(PipelineStatus.SUCCESS).build();
    }

    public static PeachOutput peachOutput() {
        return PeachOutput.builder().status(PipelineStatus.SUCCESS).build();
    }

    private static GoogleStorageLocation gsLocation(final String bucket, final String path) {
        return GoogleStorageLocation.of(bucket, path);
    }
}