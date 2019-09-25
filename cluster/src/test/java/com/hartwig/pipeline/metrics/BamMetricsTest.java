package com.hartwig.pipeline.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.resource.ResourceNames;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.stages.StageTest;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.testsupport.MockResource;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Before;

public class BamMetricsTest extends StageTest<BamMetricsOutput, SingleSampleRunMetadata> {

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockResource.addToStorage(storage, ResourceNames.REFERENCE_GENOME, "reference.fasta");
    }

    @Override
    protected Arguments createDisabledArguments() {
        return Arguments.testDefaultsBuilder().runBamMetrics(false).build();
    }

    @Override
    protected Stage<BamMetricsOutput, SingleSampleRunMetadata> createVictim() {
        return new BamMetrics(TestInputs.referenceAlignmentOutput());
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
    protected List<String> expectedResources() {
        return Collections.singletonList(resource(ResourceNames.REFERENCE_GENOME));
    }

    @Override
    protected String expectedRuntimeBucketName() {
        return "run-reference-test";
    }

    @Override
    protected List<String> expectedCommands() {
        return ImmutableList.of("java -Xmx24G -Dsamjdk.use_async_io_read_samtools=true -Dsamjdk.use_async_io_write_samtools=true "
                + "-Dsamjdk.use_async_io_write_tribble=true -Dsamjdk.buffer_size=4194304 -cp $TOOLS_DIR/gridss/2.5.2/gridss.jar "
                + "picard.cmdline.PicardCommandLine CollectWgsMetrics REFERENCE_SEQUENCE=/data/resources/reference.fasta "
                + "INPUT=/data/input/reference.bam OUTPUT=/data/output/reference.wgsmetrics "
                + "MINIMUM_MAPPING_QUALITY=20 MINIMUM_BASE_QUALITY=10 COVERAGE_CAP=250");
    }

    @Override
    protected void validateOutput(final BamMetricsOutput output) {
        GoogleStorageLocation metricsOutputFile = output.metricsOutputFile();
        assertThat(metricsOutputFile.bucket()).isEqualTo("run-reference-test/bam_metrics");
        assertThat(metricsOutputFile.path()).isEqualTo("results/reference.wgsmetrics");
    }
}