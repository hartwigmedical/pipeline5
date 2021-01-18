package com.hartwig.pipeline.sbpapi;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.execution.PipelineStatus;

import org.junit.Test;

public class SbpRunFailureTest {

    @Test
    public void failureMapsToPipelineTechnicalFailure() {
        SbpRunFailure victim = SbpRunFailure.from(PipelineStatus.FAILED);
        assertThat(victim.category()).isEqualTo("TechnicalFailure");
        assertThat(victim.type()).isEqualTo("Pipeline");
    }

    @Test
    public void qcFailureMapsToHealthCheckerQCFailure() {
        SbpRunFailure victim = SbpRunFailure.from(PipelineStatus.QC_FAILED);
        assertThat(victim.category()).isEqualTo("QCFailure");
        assertThat(victim.type()).isEqualTo("HealthCheck");
    }

    @Test(expected = IllegalArgumentException.class)
    public void unmappableStatusThrowsIllegalArgument() {
        SbpRunFailure.from(PipelineStatus.PERSISTED);
    }
}