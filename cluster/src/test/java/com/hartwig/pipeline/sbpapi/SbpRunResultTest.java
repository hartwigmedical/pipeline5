package com.hartwig.pipeline.sbpapi;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.execution.PipelineStatus;

import org.junit.Test;

public class SbpRunResultTest {

    @Test
    public void failureMapsToPipelineTechnicalFailure() {
        SbpRunResult victim = SbpRunResult.from(PipelineStatus.FAILED);
        assertThat(victim.category()).isEqualTo("TechnicalFailure");
        assertThat(victim.type()).isEqualTo("Pipeline");
    }

    @Test
    public void qcFailureMapsToHealthCheckerQCFailure() {
        SbpRunResult victim = SbpRunResult.from(PipelineStatus.QC_FAILED);
        assertThat(victim.category()).isEqualTo("QCFailure");
        assertThat(victim.type()).isEqualTo("HealthCheck");
    }

    @Test
    public void successMapsToSuccess() {
        SbpRunResult victim = SbpRunResult.from(PipelineStatus.SUCCESS);
        assertThat(victim.category()).isEqualTo("Success");
        assertThat(victim.type()).isEqualTo("Success");
    }

    @Test(expected = IllegalArgumentException.class)
    public void unmappableStatusThrowsIllegalArgument() {
        SbpRunResult.from(PipelineStatus.PERSISTED);
    }
}