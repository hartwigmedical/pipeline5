package com.hartwig.pipeline.alignment.after;

import com.hartwig.pipeline.bammetrics.BamMetricsOutput;
import com.hartwig.pipeline.execution.JobStatus;
import com.hartwig.pipeline.io.GoogleStorageLocation;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class BamMetricsOutputTest {
    @Test
    public void shouldReturnFailedStatusWhenJobFailed() {
        BamMetricsOutput output = BamMetricsOutput.builder()
                .status(JobStatus.FAILED)
                .metricsOutputFile(GoogleStorageLocation.of("bucket", "path/to/metrics.txt"))
                .build();
        assertThat(output.status()).isEqualTo(JobStatus.FAILED);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowIfJobStatusNotProvided() {
        //noinspection ResultOfMethodCallIgnored
        BamMetricsOutput.builder().build();
    }
}
