package com.hartwig.pipeline.sbpapi;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.execution.PipelineStatus;

import org.junit.Test;

public class SbpRunFailureTest {

    @Test
    public void failureMapsToPipelineTechnicalFailure() {
        SbpRunFailure victim = SbpRunFailure.from(PipelineStatus.FAILED);
        assertThat(victim.type()).isEqualTo("TechnicalFailure");
        assertThat(victim.source()).isEqualTo("Pipeline");
    }

    @Test
    public void qcFailureMapsToHealthCheckerQCFailure() {
        SbpRunFailure victim = SbpRunFailure.from(PipelineStatus.QC_FAILED);
        assertThat(victim.type()).isEqualTo("QCFailure");
        assertThat(victim.source()).isEqualTo("HealthCheck");
    }

    @Test(expected = IllegalArgumentException.class)
    public void unmappableStatusThrowsIllegalArgument() {
        SbpRunFailure.from(PipelineStatus.PERSISTED);
    }
}