package com.hartwig.pipeline.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.metadata.AddDatatype;
import com.hartwig.pipeline.metadata.ArchivePath;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.stages.StageTest;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Before;
import org.junit.Test;

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
        return new BamMetrics(TestInputs.REF_GENOME_37_RESOURCE_FILES, TestInputs.referenceAlignmentOutput(), persistedDataset);
    }

    @Override
    protected SingleSampleRunMetadata input() {
        return TestInputs.referenceRunMetadata();
    }

    @Override
    protected List<String> expectedInputs() {
        return ImmutableList.of(input("run-reference-test/aligner/results/reference.bam", "reference.bam"));
    }

    @Override
    protected String expectedRuntimeBucketName() {
        return "run-reference-test";
    }

    @Override
    protected List<String> expectedCommands() {
        return ImmutableList.of("java -Xmx24G -Dsamjdk.use_async_io_read_samtools=true -Dsamjdk.use_async_io_write_samtools=true "
                + "-Dsamjdk.use_async_io_write_tribble=true -Dsamjdk.buffer_size=4194304 -cp /opt/tools/gridss/2.13.2/gridss.jar "
                + "picard.cmdline.PicardCommandLine CollectWgsMetrics "
                + "REFERENCE_SEQUENCE=/opt/resources/reference_genome/37/Homo_sapiens.GRCh37.GATK.illumina.fasta "
                + "INPUT=/data/input/reference.bam OUTPUT=/data/output/" + REFERENCE_WGSMETRICS + " MINIMUM_MAPPING_QUALITY=20 "
                + "MINIMUM_BASE_QUALITY=10 COVERAGE_CAP=250");
    }

    /*
    @Test
    public void usesIntervalsInMetricsWhenTargetRegionsEnabled() {
        ResourceFiles resourceFiles = TestInputs.REF_GENOME_38_RESOURCE_FILES;
        resourceFiles.setTargetRegionsDir("/opt/resources/target_regions/38/");

        BamMetrics victim = new BamMetrics(resourceFiles, TestInputs.tumorAlignmentOutput(), persistedDataset);
        assertThat(victim.commands(TestInputs.tumorRunMetadata()).get(0).asBash()).isEqualTo(
                "java -Xmx1G -cp /opt/tools/gridss/2.13.2/gridss.jar picard.cmdline.PicardCommandLine "
                        + "BedToIntervalList SORT=true SEQUENCE_DICTIONARY=/opt/resources/reference_genome/38/GCA_000001405.15_GRCh38_no_alt_analysis_set.dict "
                        + "INPUT=/opt/resources/target_regions/38/target_regions.bed "
                        + "OUTPUT=/opt/resources/target_regions/38/target_regions.intervals_list");
        assertThat(victim.commands(TestInputs.tumorRunMetadata()).get(1).asBash()).isEqualTo(
                "java -Xmx24G -Dsamjdk.use_async_io_read_samtools=true -Dsamjdk.use_async_io_write_samtools=true -Dsamjdk.use_async_io_write_tribble=true -Dsamjdk.buffer_size=4194304 "
                        + "-cp /opt/tools/gridss/2.13.2/gridss.jar picard.cmdline.PicardCommandLine CollectWgsMetrics "
                        + "REFERENCE_SEQUENCE=/opt/resources/reference_genome/38/GCA_000001405.15_GRCh38_no_alt_analysis_set.fna "
                        + "INPUT=/data/input/tumor.bam OUTPUT=/data/output/tumor.wgsmetrics "
                        + "MINIMUM_MAPPING_QUALITY=20 MINIMUM_BASE_QUALITY=10 COVERAGE_CAP=250 "
                        + "INTERVALS=/opt/resources/target_regions/38/target_regions.intervals_list");
    }
    */

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