package com.hartwig.pipeline.metrics;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.input.SingleSampleRunMetadata;
import com.hartwig.computeengine.storage.GoogleStorageLocation;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.output.AddDatatype;
import com.hartwig.pipeline.output.ArchivePath;
import com.hartwig.pipeline.output.Folder;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.stages.StageTest;
import com.hartwig.pipeline.testsupport.TestInputs;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static com.hartwig.pipeline.testsupport.TestInputs.toolCommand;
import static com.hartwig.pipeline.tools.HmfTool.BAM_TOOLS;
import static org.assertj.core.api.Assertions.assertThat;

public class BamMetricsTest extends StageTest<BamMetricsOutput, SingleSampleRunMetadata> {

    public static final String REFERENCE_WGSMETRICS = "reference.wgsmetrics";

    public static final AddDatatype ADD_DATATYPE = new AddDatatype(DataType.WGSMETRICS,
            TestInputs.referenceRunMetadata().barcode(),
            new ArchivePath(Folder.from(TestInputs.referenceRunMetadata()), BamMetrics.NAMESPACE, "reference.wgsmetrics"));

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
                        + " -sample reference "
                        + "-ref_genome /opt/resources/reference_genome/37/Homo_sapiens.GRCh37.GATK.illumina.fasta "
                        + "-ref_genome_version V37 "
                        + "-bam_file /data/input/reference.bam "
                        + "-output_dir /data/output "
                        + "-log_level INFO "
                        + "-threads $(grep -c '^processor' /proc/cpuinfo) "
                        + "-write_old_style");
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
                        + " -sample tumor "
                        + "-ref_genome /opt/resources/reference_genome/38/GCA_000001405.15_GRCh38_no_alt_analysis_set.fna "
                        + "-ref_genome_version V38 "
                        + "-bam_file /data/input/tumor.bam "
                        + "-output_dir /data/output "
                        + "-log_level INFO "
                        + "-threads $(grep -c '^processor' /proc/cpuinfo) "
                        + "-write_old_style "
                        + "-regions_bed_file /opt/resources/target_regions/38/target_regions_definition.38.bed");
    }

    @Override
    protected void validateOutput(final BamMetricsOutput output) {
        GoogleStorageLocation metricsOutputFile = output.metricsOutputFile();
        assertThat(metricsOutputFile.bucket()).isEqualTo("run-reference-test/bam_metrics");
        assertThat(metricsOutputFile.path()).isEqualTo("results/" + REFERENCE_WGSMETRICS);
    }

    @Override
    protected void validatePersistedOutput(final BamMetricsOutput output) {
        assertThat(output.metricsOutputFile()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET,
                "set/reference/bam_metrics/" + REFERENCE_WGSMETRICS));
    }

    @Override
    protected void setupPersistedDataset() {
        persistedDataset.addPath(DataType.WGSMETRICS, "bam_metrics/" + REFERENCE_WGSMETRICS);
    }

    @Override
    protected void validatePersistedOutputFromPersistedDataset(final BamMetricsOutput output) {
        assertThat(output.metricsOutputFile()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET, "bam_metrics/" + REFERENCE_WGSMETRICS));
        assertThat(output.datatypes()).containsExactly(ADD_DATATYPE);
    }

    @Override
    protected List<AddDatatype> expectedFurtherOperations() {
        return List.of(ADD_DATATYPE);
    }
}