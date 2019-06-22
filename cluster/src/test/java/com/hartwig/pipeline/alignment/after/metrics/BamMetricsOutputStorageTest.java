package com.hartwig.pipeline.alignment.after.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.io.GoogleStorageLocation;
import com.hartwig.pipeline.io.ResultsDirectory;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Test;

public class BamMetricsOutputStorageTest {

    private static final ResultsDirectory RESULTS_DIRECTORY = ResultsDirectory.defaultDirectory();

    @Test
    public void returnsBamMetricsOutputWithoutCheckingStorage() {
        SingleSampleRunMetadata sample = TestInputs.referenceRunMetadata();
        String runtimeBucketName = "run-" + sample.sampleId();
        Storage storage = mock(Storage.class);
        Bucket bucket = mock(Bucket.class);
        when(bucket.getName()).thenReturn(runtimeBucketName);
        when(storage.get(runtimeBucketName)).thenReturn(bucket);
        BamMetricsOutputStorage victim = new BamMetricsOutputStorage(storage, Arguments.testDefaults(), RESULTS_DIRECTORY);
        assertThat(victim.get(TestInputs.referenceRunMetadata())).isEqualTo(BamMetricsOutput.builder()
                .status(PipelineStatus.SUCCESS)
                .sample(sample.sampleName())
                .maybeMetricsOutputFile(GoogleStorageLocation.of(runtimeBucketName + "/" + BamMetrics.NAMESPACE,
                        RESULTS_DIRECTORY.path(sample.sampleName() + ".wgsmetrics"))).build());
    }
}