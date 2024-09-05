package com.hartwig.pipeline.metrics;

import static com.hartwig.pipeline.metrics.BamMetrics.BAM_METRICS_COVERAGE_TSV;
import static com.hartwig.pipeline.metrics.BamMetrics.BAM_METRICS_FLAG_COUNT_TSV;
import static com.hartwig.pipeline.metrics.BamMetrics.BAM_METRICS_FRAG_LENGTH_TSV;
import static com.hartwig.pipeline.metrics.BamMetrics.BAM_METRICS_PARTITION_STATS_TSV;
import static com.hartwig.pipeline.metrics.BamMetrics.BAM_METRICS_SUMMARY_TSV;
import static com.hartwig.pipeline.testsupport.TestInputs.toolCommand;
import static com.hartwig.pipeline.tools.HmfTool.BAM_TOOLS;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.computeengine.storage.GoogleStorageLocation;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.input.SingleSampleRunMetadata;
import com.hartwig.pipeline.output.AddDatatype;
import com.hartwig.pipeline.output.ArchivePath;
import com.hartwig.pipeline.output.Folder;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.stages.StageTest;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Before;
import org.junit.Test;

public class BamMetricsTest extends StageTest<BamMetricsOutput, SingleSampleRunMetadata> {

    private static final String REFERENCE_SUMMARY = "reference" + BAM_METRICS_SUMMARY_TSV;
    private static final String REFERENCE_COVERAGE = "reference" + BAM_METRICS_COVERAGE_TSV;
    private static final String REFERENCE_FRAG_LENGTHS = "reference" + BAM_METRICS_FRAG_LENGTH_TSV;
    private static final String REFERENCE_FLAG_COUNTS = "reference" + BAM_METRICS_FLAG_COUNT_TSV;
    private static final String REFERENCE_PARTITION_STATS = "reference" + BAM_METRICS_PARTITION_STATS_TSV;

    public static final List<AddDatatype> ADD_DATATYPES = List.of(
            new AddDatatype(
                    DataType.METRICS_SUMMARY, TestInputs.referenceRunMetadata().barcode(),
                    new ArchivePath(Folder.from(TestInputs.referenceRunMetadata()), BamMetrics.NAMESPACE, REFERENCE_SUMMARY)),
            new AddDatatype(
                    DataType.METRICS_COVERAGE, TestInputs.referenceRunMetadata().barcode(),
                    new ArchivePath(Folder.from(TestInputs.referenceRunMetadata()), BamMetrics.NAMESPACE, REFERENCE_COVERAGE)),
            new AddDatatype(
                    DataType.METRICS_FRAG_LENGTH, TestInputs.referenceRunMetadata().barcode(),
                    new ArchivePath(Folder.from(TestInputs.referenceRunMetadata()), BamMetrics.NAMESPACE, REFERENCE_FRAG_LENGTHS)),
            new AddDatatype(
                    DataType.METRICS_FLAG_COUNT, TestInputs.referenceRunMetadata().barcode(),
                    new ArchivePath(Folder.from(TestInputs.referenceRunMetadata()), BamMetrics.NAMESPACE, REFERENCE_FLAG_COUNTS)),
            new AddDatatype(
                    DataType.METRICS_PARTITION, TestInputs.referenceRunMetadata().barcode(),
                    new ArchivePath(Folder.from(TestInputs.referenceRunMetadata()), BamMetrics.NAMESPACE, REFERENCE_PARTITION_STATS))
            );

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected Arguments createDisabledArguments() {
        return Arguments.testDefaultsBuilder().runBamMetrics(false).build();
    }

    @Override
    protected Stage<BamMetricsOutput, SingleSampleRunMetadata> createVictim() {
        return new BamMetrics(TestInputs.REF_GENOME_37_RESOURCE_FILES,
                TestInputs.referenceAlignmentOutput(),
                persistedDataset,
                Arguments.testDefaults());
    }

    @Override
    protected SingleSampleRunMetadata input() {
        return TestInputs.referenceRunMetadata();
    }

    @Override
    protected List<String> expectedInputs() {
        return ImmutableList.of(
                input("run-reference-test/aligner/results/reference.bam", "reference.bam"),
                input("run-reference-test/aligner/results/reference.bam.bai", "reference.bam.bai"));
    }

    @Override
    protected String expectedRuntimeBucketName() {
        return "run-reference-test";
    }

    @Override
    protected List<String> expectedCommands() {

        return ImmutableList.of(
                toolCommand(BAM_TOOLS)
                + " -sample reference"
                + " -ref_genome /opt/resources/reference_genome/37/Homo_sapiens.GRCh37.GATK.illumina.fasta"
                + " -ref_genome_version V37"
                + " -bam_file /data/input/reference.bam"
                + " -output_dir /data/output"
                + " -log_level INFO"
                + " -threads $(grep -c '^processor' /proc/cpuinfo)");
    }

    @Test
    public void usesIntervalsInMetricsWhenTargetRegionsEnabled() {
        ResourceFiles resourceFiles = TestInputs.REF_GENOME_38_RESOURCE_FILES;
        BamMetrics victim = new BamMetrics(resourceFiles,
                TestInputs.tumorAlignmentOutput(),
                persistedDataset,
                Arguments.testDefaultsBuilder().useTargetRegions(true).build());

        assertThat(victim.tumorReferenceCommands(TestInputs.tumorRunMetadata()).get(0).asBash()).isEqualTo(
                toolCommand(BAM_TOOLS)
                        + " -sample tumor"
                        + " -ref_genome /opt/resources/reference_genome/38/GCA_000001405.15_GRCh38_no_alt_analysis_set.fna"
                        + " -ref_genome_version V38"
                        + " -bam_file /data/input/tumor.bam"
                        + " -output_dir /data/output"
                        + " -log_level INFO"
                        + " -threads $(grep -c '^processor' /proc/cpuinfo)"
                        + " -regions_file /opt/resources/target_regions/38/target_regions_definition.38.bed");
    }

    @Override
    protected void validateOutput(final BamMetricsOutput output) {
        GoogleStorageLocation metricsOutputFile = output.outputLocations().summary();
        assertThat(metricsOutputFile.bucket()).isEqualTo("run-reference-test/bam_metrics");
        assertThat(metricsOutputFile.path()).isEqualTo("results/" + REFERENCE_SUMMARY);
    }

    @Override
    protected void validatePersistedOutput(final BamMetricsOutput output) {
        assertThat(output.outputLocations().summary()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET,
                "set/reference/bam_metrics/" + REFERENCE_SUMMARY));
    }

    @Override
    protected void setupPersistedDataset() {
        persistedDataset.addPath(DataType.METRICS_SUMMARY, "bam_metrics/" + REFERENCE_SUMMARY);
    }

    @Override
    protected void validatePersistedOutputFromPersistedDataset(final BamMetricsOutput output) {
        assertThat(output.outputLocations().summary()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET, "bam_metrics/" + REFERENCE_SUMMARY));
    }

    @Override
    protected List<AddDatatype> expectedFurtherOperations() {
        return ADD_DATATYPES;
    }
}