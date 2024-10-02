package com.hartwig.pipeline.testsupport;

import static java.lang.String.format;

import static com.hartwig.pipeline.alignment.redux.Redux.jitterParamsTsv;
import static com.hartwig.pipeline.alignment.redux.Redux.msTableTsv;
import static com.hartwig.pipeline.calling.structural.SvCalling.ESVEE_ALIGNMENT_TSV;
import static com.hartwig.pipeline.calling.structural.SvCalling.ESVEE_ASSEMBLY_TSV;
import static com.hartwig.pipeline.calling.structural.SvCalling.ESVEE_BREAKEND_TSV;
import static com.hartwig.pipeline.calling.structural.SvCalling.ESVEE_FRAG_LENGTH_TSV;
import static com.hartwig.pipeline.calling.structural.SvCalling.ESVEE_GERMLINE_VCF;
import static com.hartwig.pipeline.calling.structural.SvCalling.ESVEE_PHASED_ASSEMBLY_TSV;
import static com.hartwig.pipeline.calling.structural.SvCalling.ESVEE_PREP_BAM_FILE;
import static com.hartwig.pipeline.calling.structural.SvCalling.ESVEE_PREP_INDEX_FILE;
import static com.hartwig.pipeline.calling.structural.SvCalling.ESVEE_PREP_JUNCTION_TSV;
import static com.hartwig.pipeline.calling.structural.SvCalling.ESVEE_SOMATIC_VCF;
import static com.hartwig.pipeline.calling.structural.SvCalling.ESVEE_UNFILTERED_VCF;
import static com.hartwig.pipeline.datatypes.FileTypes.bam;
import static com.hartwig.pipeline.metrics.BamMetrics.BAM_METRICS_COVERAGE_TSV;
import static com.hartwig.pipeline.metrics.BamMetrics.BAM_METRICS_FLAG_COUNT_TSV;
import static com.hartwig.pipeline.metrics.BamMetrics.BAM_METRICS_FRAG_LENGTH_TSV;
import static com.hartwig.pipeline.metrics.BamMetrics.BAM_METRICS_PARTITION_STATS_TSV;
import static com.hartwig.pipeline.metrics.BamMetrics.BAM_METRICS_SUMMARY_TSV;

import java.time.LocalDate;
import java.util.List;

import com.hartwig.computeengine.storage.GoogleStorageLocation;
import com.hartwig.pdl.OperationalReferences;
import com.hartwig.pdl.PipelineInput;
import com.hartwig.pdl.SampleInput;
import com.hartwig.pipeline.PipelineStatus;
import com.hartwig.pipeline.alignment.Aligner;
import com.hartwig.pipeline.alignment.AlignmentOutput;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.calling.germline.GermlineCaller;
import com.hartwig.pipeline.calling.germline.GermlineCallerOutput;
import com.hartwig.pipeline.calling.sage.SageCaller;
import com.hartwig.pipeline.calling.sage.SageConfiguration;
import com.hartwig.pipeline.calling.sage.SageOutput;
import com.hartwig.pipeline.calling.structural.Esvee;
import com.hartwig.pipeline.calling.structural.EsveeOutput;
import com.hartwig.pipeline.cram.CramOutput;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.input.SingleSampleRunMetadata;
import com.hartwig.pipeline.input.SomaticRunMetadata;
import com.hartwig.pipeline.metrics.BamMetrics;
import com.hartwig.pipeline.metrics.BamMetricsOutput;
import com.hartwig.pipeline.metrics.BamMetricsOutputLocations;
import com.hartwig.pipeline.resource.RefGenome37ResourceFiles;
import com.hartwig.pipeline.resource.RefGenome38ResourceFiles;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.snpgenotype.SnpGenotypeOutput;
import com.hartwig.pipeline.tertiary.amber.Amber;
import com.hartwig.pipeline.tertiary.amber.AmberOutput;
import com.hartwig.pipeline.tertiary.chord.Chord;
import com.hartwig.pipeline.tertiary.chord.ChordOutput;
import com.hartwig.pipeline.tertiary.chord.ChordOutputLocations;
import com.hartwig.pipeline.tertiary.cobalt.Cobalt;
import com.hartwig.pipeline.tertiary.cobalt.CobaltOutput;
import com.hartwig.pipeline.tertiary.cuppa.Cuppa;
import com.hartwig.pipeline.tertiary.cuppa.CuppaOutput;
import com.hartwig.pipeline.tertiary.cuppa.CuppaOutputLocations;
import com.hartwig.pipeline.tertiary.healthcheck.HealthCheckOutput;
import com.hartwig.pipeline.tertiary.healthcheck.HealthChecker;
import com.hartwig.pipeline.tertiary.lilac.Lilac;
import com.hartwig.pipeline.tertiary.lilac.LilacBamSliceOutput;
import com.hartwig.pipeline.tertiary.lilac.LilacBamSlicer;
import com.hartwig.pipeline.tertiary.lilac.LilacOutput;
import com.hartwig.pipeline.tertiary.linx.LinxGermline;
import com.hartwig.pipeline.tertiary.linx.LinxGermlineOutput;
import com.hartwig.pipeline.tertiary.linx.LinxGermlineOutputLocations;
import com.hartwig.pipeline.tertiary.linx.LinxSomatic;
import com.hartwig.pipeline.tertiary.linx.LinxSomaticOutput;
import com.hartwig.pipeline.tertiary.linx.LinxSomaticOutputLocations;
import com.hartwig.pipeline.tertiary.orange.OrangeOutput;
import com.hartwig.pipeline.tertiary.pave.PaveGermline;
import com.hartwig.pipeline.tertiary.pave.PaveOutput;
import com.hartwig.pipeline.tertiary.pave.PaveSomatic;
import com.hartwig.pipeline.tertiary.peach.Peach;
import com.hartwig.pipeline.tertiary.peach.PeachOutput;
import com.hartwig.pipeline.tertiary.purple.Purple;
import com.hartwig.pipeline.tertiary.purple.PurpleOutput;
import com.hartwig.pipeline.tertiary.purple.PurpleOutputLocations;
import com.hartwig.pipeline.tertiary.sigs.Sigs;
import com.hartwig.pipeline.tertiary.sigs.SigsOutput;
import com.hartwig.pipeline.tertiary.virus.VirusBreakend;
import com.hartwig.pipeline.tertiary.virus.VirusBreakendOutput;
import com.hartwig.pipeline.tertiary.virus.VirusInterpreter;
import com.hartwig.pipeline.tertiary.virus.VirusInterpreterOutput;
import com.hartwig.pipeline.tools.HmfTool;

import org.jetbrains.annotations.NotNull;

@SuppressWarnings("unused")
public class TestInputs {

    public static final ResourceFiles REF_GENOME_37_RESOURCE_FILES = new RefGenome37ResourceFiles();
    public static final ResourceFiles REF_GENOME_38_RESOURCE_FILES = new RefGenome38ResourceFiles();
    public static final String SET = "set";
    public static final String BUCKET = "bucket";
    public static final long EXTERNAL_RUN_ID = 1L;
    public static final long EXTERNAL_SET_ID = 2L;
    private static final String RESULTS = "results/";
    private static final String REFERENCE_SAMPLE = "reference";
    public static final String REFERENCE_BUCKET = "run-" + REFERENCE_SAMPLE + "-test";
    private static final String TUMOR_SAMPLE = "tumor";
    public static final String TUMOR_BUCKET = "run-" + TUMOR_SAMPLE + "-test";
    public static final String SOMATIC_BUCKET = "run-" + REFERENCE_SAMPLE + "-" + TUMOR_SAMPLE + "-test";

    public static PipelineInput pipelineInput() {
        return PipelineInput.builder()
                .setName(SET)
                .reference(SampleInput.builder().name(REFERENCE_SAMPLE).turquoiseSubject(REFERENCE_SAMPLE).build())
                .tumor(SampleInput.builder().name(TUMOR_SAMPLE).turquoiseSubject(REFERENCE_SAMPLE).build())
                .build();
    }

    public static String inputDownload(final String commands) {
        return "gsutil -o 'GSUtil:parallel_thread_count=1' -o GSUtil:sliced_object_download_max_components=$(nproc) -qm " + commands;
    }

    public static String toolCommand(final HmfTool hmfTool) {
        return format("java -Xmx%dG -jar /opt/tools/%s/%s/%s",
                hmfTool.getMaxHeap(),
                hmfTool.directory(),
                hmfTool.runVersion(),
                hmfTool.jar());
    }

    public static String toolCommand(final HmfTool hmfTool, final String classPath) {
        return format("java -Xmx%dG -cp /opt/tools/%s/%s/%s %s",
                hmfTool.getMaxHeap(),
                hmfTool.directory(),
                hmfTool.runVersion(),
                hmfTool.jar(),
                classPath);
    }

    public static String referenceSample() {
        return REFERENCE_SAMPLE;
    }

    public static String tumorSample() {
        return TUMOR_SAMPLE;
    }

    public static SomaticRunMetadata defaultSomaticRunMetadata() {
        final SingleSampleRunMetadata tumor = tumorRunMetadata();
        final SingleSampleRunMetadata reference = referenceRunMetadata();
        return SomaticRunMetadata.builder()
                .set(SET)
                .maybeTumor(tumor)
                .maybeReference(reference)
                .bucket(BUCKET)
                .maybeExternalIds(externalIds())
                .build();
    }

    public static OperationalReferences externalIds() {
        return OperationalReferences.builder().runId(EXTERNAL_RUN_ID).setId(EXTERNAL_SET_ID).build();
    }

    public static SomaticRunMetadata defaultSingleSampleRunMetadata() {
        final SingleSampleRunMetadata reference = referenceRunMetadata();
        return SomaticRunMetadata.builder().set(SET).maybeReference(reference).bucket(BUCKET).maybeExternalIds(externalIds()).build();
    }

    @NotNull
    public static SingleSampleRunMetadata referenceRunMetadata() {
        return SingleSampleRunMetadata.builder()
                .set(SET)
                .bucket(BUCKET)
                .type(SingleSampleRunMetadata.SampleType.REFERENCE)
                .barcode(referenceAlignmentOutput().sample())
                .turquoiseSubject(referenceAlignmentOutput().sample())
                .build();
    }

    @NotNull
    public static SingleSampleRunMetadata tumorRunMetadata() {
        return SingleSampleRunMetadata.builder()
                .set(SET)
                .bucket(BUCKET)
                .type(SingleSampleRunMetadata.SampleType.TUMOR)
                .barcode(tumorAlignmentOutput().sample())
                .turquoiseSubject(tumorAlignmentOutput().sample())
                .primaryTumorDoids(List.of("01", "02"))
                .samplingDate(LocalDate.of(2023, 5, 19))
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
                .maybeAlignments(gsLocation(bucket, RESULTS + bam(sample)))
                .maybeJitterParams(gsLocation(bucket, RESULTS + jitterParamsTsv(sample)))
                .maybeMsTable(gsLocation(bucket, RESULTS + msTableTsv(sample)))
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
                .maybeOutputLocations(BamMetricsOutputLocations.builder()
                        .outputDirectory(gsLocation(namespacedBucket(sample, BamMetrics.NAMESPACE), RESULTS))
                        .summary(gsLocation(namespacedBucket(sample, BamMetrics.NAMESPACE), sample + BAM_METRICS_SUMMARY_TSV))
                        .coverage(gsLocation(namespacedBucket(sample, BamMetrics.NAMESPACE), sample + BAM_METRICS_COVERAGE_TSV))
                        .fragmentLengths(gsLocation(namespacedBucket(sample, BamMetrics.NAMESPACE), sample + BAM_METRICS_FRAG_LENGTH_TSV))
                        .flagCounts(gsLocation(namespacedBucket(sample, BamMetrics.NAMESPACE), sample + BAM_METRICS_FLAG_COUNT_TSV))
                        .partitionStats(gsLocation(namespacedBucket(sample, BamMetrics.NAMESPACE), sample + BAM_METRICS_PARTITION_STATS_TSV))
                        .build())
                .build();
    }

    public static SageOutput sageGermlineOutput() {
        return SageOutput.builder(SageConfiguration.SAGE_GERMLINE_NAMESPACE)
                .status(PipelineStatus.SUCCESS)
                .maybeVariants(gsLocation(somaticBucket(SageConfiguration.SAGE_GERMLINE_NAMESPACE),
                        RESULTS + TUMOR_SAMPLE + ".germline." + FileTypes.GZIPPED_VCF))
                .maybeGermlineGeneCoverage(gsLocation(somaticBucket(SageConfiguration.SAGE_GERMLINE_NAMESPACE),
                        RESULTS + TUMOR_SAMPLE + SageCaller.SAGE_GENE_COVERAGE_TSV))
                .build();
    }

    public static SageOutput sageSomaticOutput() {
        return SageOutput.builder(SageConfiguration.SAGE_SOMATIC_NAMESPACE)
                .status(PipelineStatus.SUCCESS)
                .maybeVariants(gsLocation(somaticBucket(SageConfiguration.SAGE_SOMATIC_NAMESPACE),
                        RESULTS + TUMOR_SAMPLE + ".somatic." + FileTypes.GZIPPED_VCF))
                .maybeSomaticRefSampleBqrPlot(gsLocation(somaticBucket(SageConfiguration.SAGE_SOMATIC_NAMESPACE),
                        RESULTS + REFERENCE_SAMPLE + SageCaller.SAGE_BQR_PNG))
                .maybeSomaticTumorSampleBqrPlot(gsLocation(somaticBucket(SageConfiguration.SAGE_SOMATIC_NAMESPACE),
                        RESULTS + TUMOR_SAMPLE + SageCaller.SAGE_BQR_PNG))
                .build();
    }

    public static PaveOutput paveSomaticOutput() {
        return PaveOutput.builder(PaveSomatic.NAMESPACE)
                .status(PipelineStatus.SUCCESS)
                .maybeAnnotatedVariants(gsLocation(somaticBucket(PaveSomatic.NAMESPACE),
                        RESULTS + TUMOR_SAMPLE + ".somatic." + FileTypes.GZIPPED_VCF))
                .build();
    }

    public static PaveOutput paveGermlineOutput() {
        return PaveOutput.builder(PaveGermline.NAMESPACE)
                .status(PipelineStatus.SUCCESS)
                .maybeAnnotatedVariants(gsLocation(somaticBucket(PaveGermline.NAMESPACE),
                        RESULTS + TUMOR_SAMPLE + ".germline." + FileTypes.GZIPPED_VCF))
                .build();
    }

    private static String formTumorFilename(final String esveeFileSuffix)
    {
        return RESULTS + TUMOR_SAMPLE + "." + esveeFileSuffix;
    }

    public static EsveeOutput esveeOutput() {
        String unfilteredVcf = "." + ESVEE_UNFILTERED_VCF;
        String somaticVcf = "." + ESVEE_SOMATIC_VCF;
        String germlineVcf = "." + ESVEE_GERMLINE_VCF;

        return EsveeOutput.builder()
                .status(PipelineStatus.SUCCESS)
                .maybePrepBam(gsLocation(somaticBucket(Esvee.NAMESPACE),formTumorFilename(ESVEE_PREP_BAM_FILE)))
                .maybePrepBamIndex(gsLocation(somaticBucket(Esvee.NAMESPACE),formTumorFilename(ESVEE_PREP_INDEX_FILE)))
                .maybePrepJunctionTsv(gsLocation(somaticBucket(Esvee.NAMESPACE),formTumorFilename(ESVEE_PREP_JUNCTION_TSV)))
                .maybeFragLengths(gsLocation(somaticBucket(Esvee.NAMESPACE),formTumorFilename(ESVEE_FRAG_LENGTH_TSV)))
                .maybeAssemblyTsv(gsLocation(somaticBucket(Esvee.NAMESPACE),formTumorFilename(ESVEE_ASSEMBLY_TSV)))
                .maybePhasedAssemblyTsv(gsLocation(somaticBucket(Esvee.NAMESPACE),formTumorFilename(ESVEE_PHASED_ASSEMBLY_TSV)))
                .maybeBreakendTsv(gsLocation(somaticBucket(Esvee.NAMESPACE),formTumorFilename(ESVEE_ALIGNMENT_TSV)))
                .maybeAlignmentTsv(gsLocation(somaticBucket(Esvee.NAMESPACE),formTumorFilename(ESVEE_BREAKEND_TSV)))
                .maybeUnfilteredVcf(gsLocation(somaticBucket(Esvee.NAMESPACE), formTumorFilename(ESVEE_UNFILTERED_VCF)))
                .maybeSomaticVcf(gsLocation(somaticBucket(Esvee.NAMESPACE), formTumorFilename(ESVEE_SOMATIC_VCF)))
                .maybeGermlineVcf(gsLocation(somaticBucket(Esvee.NAMESPACE), formTumorFilename(ESVEE_GERMLINE_VCF)))
                .build();
    }

    public static VirusBreakendOutput virusBreakendOutput() {
        return VirusBreakendOutput.builder()
                .status(PipelineStatus.SUCCESS)
                .maybeSummary(gsLocation(somaticBucket(VirusBreakend.NAMESPACE), TUMOR_SAMPLE + ".virusbreakend.vcf.summary.tsv"))
                .maybeVariants(gsLocation(somaticBucket(VirusBreakend.NAMESPACE), TUMOR_SAMPLE + ".virusbreakend.vcf"))
                .build();
    }

    public static VirusInterpreterOutput virusInterpreterOutput() {
        return VirusInterpreterOutput.builder()
                .status(PipelineStatus.SUCCESS)
                .maybeVirusAnnotations(gsLocation(somaticBucket(VirusInterpreter.NAMESPACE), TUMOR_SAMPLE + ".virus.annotated.tsv"))
                .build();
    }

    private static String somaticBucket(final String namespace) {
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
                        .somaticVariants(gsLocation(somaticBucket(Purple.NAMESPACE), TUMOR_SAMPLE + Purple.PURPLE_SOMATIC_VCF))
                        .germlineVariants(gsLocation(somaticBucket(Purple.NAMESPACE), TUMOR_SAMPLE + Purple.PURPLE_GERMLINE_VCF))
                        .structuralVariants(gsLocation(somaticBucket(Purple.NAMESPACE), TUMOR_SAMPLE + Purple.PURPLE_SOMATIC_SV_VCF))
                        .germlineStructuralVariants(gsLocation(somaticBucket(Purple.NAMESPACE),
                                TUMOR_SAMPLE + Purple.PURPLE_GERMLINE_SV_VCF))
                        .purity(gsLocation(somaticBucket(Purple.NAMESPACE), TUMOR_SAMPLE + Purple.PURPLE_PURITY_TSV))
                        .qcFile(gsLocation(somaticBucket(Purple.NAMESPACE), TUMOR_SAMPLE + Purple.PURPLE_QC))
                        .geneCopyNumber(gsLocation(somaticBucket(Purple.NAMESPACE), TUMOR_SAMPLE + Purple.PURPLE_GENE_COPY_NUMBER_TSV))
                        .somaticDriverCatalog(gsLocation(somaticBucket(Purple.NAMESPACE),
                                TUMOR_SAMPLE + Purple.PURPLE_SOMATIC_DRIVER_CATALOG))
                        .germlineDriverCatalog(gsLocation(somaticBucket(Purple.NAMESPACE),
                                TUMOR_SAMPLE + Purple.PURPLE_GERMLINE_DRIVER_CATALOG))
                        .circosPlot(gsLocation(somaticBucket(Purple.NAMESPACE),
                                format("plot/%s%s", TUMOR_SAMPLE, Purple.PURPLE_CIRCOS_PLOT)))
                        .somaticCopyNumber(gsLocation(somaticBucket(Purple.NAMESPACE),
                                TUMOR_SAMPLE + Purple.PURPLE_SOMATIC_COPY_NUMBER_TSV))
                        .germlineDeletions(gsLocation(somaticBucket(Purple.NAMESPACE), TUMOR_SAMPLE + Purple.PURPLE_GERMLINE_DELETION_TSV))
                        .build())
                .build();
    }

    public static ChordOutput chordOutput() {
        return ChordOutput.builder()
                .status(PipelineStatus.SUCCESS)
                .maybeChordOutputLocations(ChordOutputLocations.builder()
                        .predictions(GoogleStorageLocation.of(somaticBucket(Chord.NAMESPACE), TUMOR_SAMPLE + Chord.PREDICTION_TXT))
                        .signatures(GoogleStorageLocation.of(somaticBucket(Chord.NAMESPACE), TUMOR_SAMPLE + Chord.SIGNATURES_TXT))
                        .build())
                .build();
    }

    public static CuppaOutput cuppaOutput() {
        return CuppaOutput.builder()
                .status(PipelineStatus.SUCCESS)
                .maybeCuppaOutputLocations(CuppaOutputLocations.builder()
                        .visData(GoogleStorageLocation.of(somaticBucket(Cuppa.NAMESPACE), TUMOR_SAMPLE + Cuppa.CUPPA_VIS_DATA))
                        .visPlot(GoogleStorageLocation.of(somaticBucket(Cuppa.NAMESPACE), TUMOR_SAMPLE + Cuppa.CUPPA_VIS_PLOT))
                        .predSumm(GoogleStorageLocation.of(somaticBucket(Cuppa.NAMESPACE), TUMOR_SAMPLE + Cuppa.CUPPA_PRED_SUMM))
                        .build())
                .build();
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

    public static LinxSomaticOutput linxSomaticOutput() {
        return LinxSomaticOutput.builder()
                .status(PipelineStatus.SUCCESS)
                .maybeLinxOutputLocations(LinxSomaticOutputLocations.builder()
                        .breakends(gsLocation(somaticBucket(LinxSomatic.NAMESPACE), TUMOR_SAMPLE + LinxSomatic.BREAKEND_TSV))
                        .driverCatalog(gsLocation(somaticBucket(LinxSomatic.NAMESPACE), TUMOR_SAMPLE + LinxSomatic.DRIVER_CATALOG_TSV))
                        .fusions(gsLocation(somaticBucket(LinxSomatic.NAMESPACE), TUMOR_SAMPLE + LinxSomatic.FUSION_TSV))
                        .svAnnotations(gsLocation(somaticBucket(LinxSomatic.NAMESPACE), TUMOR_SAMPLE + LinxSomatic.SV_ANNOTATIONS_TSV))
                        .clusters(gsLocation(somaticBucket(LinxSomatic.NAMESPACE), TUMOR_SAMPLE + LinxSomatic.CLUSTERS_TSV))
                        .outputDirectory(gsLocation(somaticBucket(LinxSomatic.NAMESPACE), RESULTS))
                        .drivers(gsLocation(somaticBucket(LinxSomatic.NAMESPACE), TUMOR_SAMPLE + LinxSomatic.DRIVERS_TSV))
                        .build())
                .build();
    }

    public static LinxGermlineOutput linxGermlineOutput() {
        return LinxGermlineOutput.builder()
                .status(PipelineStatus.SUCCESS)
                .maybeLinxGermlineOutputLocations(LinxGermlineOutputLocations.builder()
                        .disruptions(gsLocation(somaticBucket(LinxGermline.NAMESPACE), TUMOR_SAMPLE + LinxGermline.GERMLINE_DISRUPTION_TSV))
                        .breakends(gsLocation(somaticBucket(LinxGermline.NAMESPACE), TUMOR_SAMPLE + LinxGermline.GERMLINE_BREAKEND_TSV))
                        .driverCatalog(gsLocation(somaticBucket(LinxGermline.NAMESPACE),
                                TUMOR_SAMPLE + LinxGermline.GERMLINE_DRIVER_CATALOG_TSV))
                        .outputDirectory(gsLocation(somaticBucket(LinxGermline.NAMESPACE), RESULTS))
                        .build())
                .build();
    }

    public static LilacOutput lilacOutput() {
        return LilacOutput.builder()
                .status(PipelineStatus.SUCCESS)
                .qc(GoogleStorageLocation.of(somaticBucket(Lilac.NAMESPACE), TUMOR_SAMPLE + ".lilac.qc.tsv"))
                .result(GoogleStorageLocation.of(somaticBucket(Lilac.NAMESPACE), TUMOR_SAMPLE + ".lilac.tsv"))
                .build();
    }

    public static LilacBamSliceOutput lilacBamSliceOutput() {
        return LilacBamSliceOutput.builder()
                .status(PipelineStatus.SUCCESS)
                .reference(GoogleStorageLocation.of(somaticBucket(LilacBamSlicer.NAMESPACE), REFERENCE_SAMPLE + ".hla.bam"))
                .referenceIndex(GoogleStorageLocation.of(somaticBucket(LilacBamSlicer.NAMESPACE), REFERENCE_SAMPLE + ".hla.bam.bai"))
                .tumor(GoogleStorageLocation.of(somaticBucket(LilacBamSlicer.NAMESPACE), TUMOR_SAMPLE + ".hla.bam"))
                .tumorIndex(GoogleStorageLocation.of(somaticBucket(LilacBamSlicer.NAMESPACE), TUMOR_SAMPLE + ".hla.bam.bai"))
                .build();
    }

    public static PeachOutput peachOutput() {
        return PeachOutput.builder()
                .status(PipelineStatus.SUCCESS)
                .maybeGenotypes(GoogleStorageLocation.of(somaticBucket(Peach.NAMESPACE), REFERENCE_SAMPLE + Peach.PEACH_GENOTYPE_TSV))
                .build();
    }

    public static OrangeOutput orangeOutput() {
        return OrangeOutput.builder().status(PipelineStatus.SUCCESS).build();
    }

    public static SigsOutput sigsOutput() {
        return SigsOutput.builder()
                .maybeAllocationTsv(GoogleStorageLocation.of(somaticBucket(Sigs.NAMESPACE), TUMOR_SAMPLE + Sigs.ALLOCATION_TSV))
                .status(PipelineStatus.SUCCESS)
                .build();
    }

    private static GoogleStorageLocation gsLocation(final String bucket, final String path) {
        return GoogleStorageLocation.of(bucket, path);
    }
}