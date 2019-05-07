package com.hartwig.pipeline.alignment.after.metrics;

import com.hartwig.pipeline.execution.JobStatus;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class BamMetricsOutputTest {
    @Test
    public void shouldReturnFailedStatusWhenJobFailed() {
        BamMetricsOutput output = ImmutableBamMetricsOutput.builder().status(JobStatus.FAILED).build();
        assertThat(output.status()).isEqualTo(JobStatus.FAILED);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowIfJobStatusNotProvided() {
        ImmutableBamMetricsOutput.builder().build();
    }

    @Test
    public void shouldReturnEmptyOptionalIfWgsMetricsNeverSet() {
        BamMetricsOutput output = ImmutableBamMetricsOutput.builder().status(JobStatus.UNKNOWN).build();
        assertThat(output.maybeMetricsOutputFile()).isNotNull();
        assertThat(output.maybeMetricsOutputFile()).isEmpty();
    }
}
