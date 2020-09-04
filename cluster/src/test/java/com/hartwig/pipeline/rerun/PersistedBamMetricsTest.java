package com.hartwig.pipeline.rerun;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.metrics.BamMetrics;
import com.hartwig.pipeline.metrics.BamMetricsOutput;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.testsupport.TestInputs;

public class PersistedBamMetricsTest extends AbstractPersistedStageTest<BamMetricsOutput, SingleSampleRunMetadata> {

    @Override
    protected void assertOutput(final BamMetricsOutput output) {
        assertThat(output.metricsOutputFile()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET,
                "set/reference/bam_metrics/reference.wgsmetrics"));
    }

    @Override
    protected SingleSampleRunMetadata metadata() {
        return TestInputs.referenceRunMetadata();
    }

    @Override
    protected PersistedStage<BamMetricsOutput, SingleSampleRunMetadata> createStage(
            final Stage<BamMetricsOutput, SingleSampleRunMetadata> decorated, final Arguments testDefaults, final String set) {
        return new PersistedBamMetrics(decorated, testDefaults, set);
    }

    @Override
    protected String namespace() {
        return BamMetrics.NAMESPACE;
    }
}