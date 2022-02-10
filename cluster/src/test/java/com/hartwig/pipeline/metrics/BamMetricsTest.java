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
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.stages.StageTest;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Before;

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