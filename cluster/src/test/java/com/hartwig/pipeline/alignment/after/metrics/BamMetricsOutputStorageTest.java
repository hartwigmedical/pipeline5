package com.hartwig.pipeline.alignment.after.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.alignment.after.metrics.BamMetricsOutputStorage;
import com.hartwig.pipeline.execution.JobStatus;
import com.hartwig.pipeline.io.GoogleStorageLocation;
import com.hartwig.pipeline.io.ResultsDirectory;
import com.hartwig.pipeline.testsupport.TestSamples;

import org.junit.Test;

public class BamMetricsOutputStorageTest {

    private static final ResultsDirectory RESULTS_DIRECTORY = ResultsDirectory.defaultDirectory();

    @Test
    public void returnsBamMetricsOutputWithoutCheckingStorage() {
        Sample sample = TestSamples.simpleReferenceSample();
        String runtimeBucketName = "run-" + sample.name();
        Storage storage = mock(Storage.class);
        Bucket bucket = mock(Bucket.class);
        when(bucket.getName()).thenReturn(runtimeBucketName);
        when(storage.get(runtimeBucketName)).thenReturn(bucket);
        BamMetricsOutputStorage victim = new BamMetricsOutputStorage(storage, Arguments.testDefaults(), RESULTS_DIRECTORY);
        assertThat(victim.get(sample)).isEqualTo(BamMetricsOutput.builder()
                .status(JobStatus.SUCCESS)
                .maybeMetricsOutputFile(GoogleStorageLocation.of(runtimeBucketName + "/" + BamMetrics.NAMESPACE,
                        RESULTS_DIRECTORY.path(sample.name() + ".wgsmetrics"))).build());
    }
}