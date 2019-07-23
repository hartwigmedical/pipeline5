package com.hartwig.pipeline.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.testsupport.TestBlobs;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Before;
import org.junit.Test;

public class BamMetricsOutputStorageTest {

    private static final ResultsDirectory RESULTS_DIRECTORY = ResultsDirectory.defaultDirectory();
    private SingleSampleRunMetadata sample;
    private BamMetricsOutputStorage victim;
    private Bucket bucket;
    private String runtimeBucketName;

    @Before
    public void setUp() throws Exception {
        sample = TestInputs.referenceRunMetadata();
        runtimeBucketName = "run-" + sample.sampleId();
        final Storage storage = mock(Storage.class);
        bucket = mock(Bucket.class);
        when(bucket.getName()).thenReturn(runtimeBucketName);
        when(storage.get(runtimeBucketName)).thenReturn(bucket);
        victim = new BamMetricsOutputStorage(storage, Arguments.testDefaults(), RESULTS_DIRECTORY, 2, 1);
    }

    @Test
    public void waitsForBamMetricsOutput() {
        String blobName = BamMetricsOutput.outputFile(sample.sampleName());
        Blob metricsBlob = TestBlobs.blob(blobName);
        when(bucket.get("bam_metrics/results/" + blobName)).thenReturn(null).thenReturn(metricsBlob);
        assertThat(victim.get(TestInputs.referenceRunMetadata())).isEqualTo(BamMetricsOutput.builder()
                .status(PipelineStatus.SUCCESS)
                .sample(sample.sampleName())
                .maybeMetricsOutputFile(GoogleStorageLocation.of(runtimeBucketName + "/" + BamMetrics.NAMESPACE,
                        RESULTS_DIRECTORY.path(blobName)))
                .build());
    }

    @Test(expected = IllegalStateException.class)
    public void throwIllegalStateIfNoBamMetricsBeforeTimeout() {
        victim.get(TestInputs.referenceRunMetadata());
    }
}