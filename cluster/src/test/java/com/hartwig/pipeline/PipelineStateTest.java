package com.hartwig.pipeline;

import com.hartwig.computeengine.execution.ComputeEngineStatus;
import com.hartwig.computeengine.storage.GoogleStorageLocation;
import com.hartwig.pipeline.output.AddDatatype;
import com.hartwig.pipeline.output.OutputComponent;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class PipelineStateTest {

    @Test
    public void statusSuccessWhenAllStatusSuccessOrSkipped() {
        PipelineState victim = new PipelineState();
        victim.add(output(ComputeEngineStatus.SUCCESS));
        victim.add(output(ComputeEngineStatus.SKIPPED));
        assertThat(victim.shouldProceed()).isTrue();
        assertThat(victim.status()).isEqualTo(ComputeEngineStatus.SUCCESS);
    }

    @Test
    public void statusFailedWhenAnyStatusFails() {
        PipelineState victim = new PipelineState();
        victim.add(output(ComputeEngineStatus.SUCCESS));
        victim.add(output(ComputeEngineStatus.SKIPPED));
        victim.add(output(ComputeEngineStatus.QC_FAILED));
        victim.add(output(ComputeEngineStatus.FAILED));
        assertThat(victim.shouldProceed()).isFalse();
        assertThat(victim.status()).isEqualTo(ComputeEngineStatus.FAILED);
    }

    @Test
    public void statusFailedWhenAnyStatusQcFails() {
        PipelineState victim = new PipelineState();
        victim.add(output(ComputeEngineStatus.SUCCESS));
        victim.add(output(ComputeEngineStatus.SKIPPED));
        victim.add(output(ComputeEngineStatus.QC_FAILED));
        assertThat(victim.shouldProceed()).isTrue();
        assertThat(victim.status()).isEqualTo(ComputeEngineStatus.QC_FAILED);
    }

    @NotNull
    public StageOutput output(final ComputeEngineStatus status) {
        return new StageOutput() {
            @Override
            public String name() {
                return "test";
            }

            @Override
            public ComputeEngineStatus status() {
                return status;
            }

            @Override
            public List<OutputComponent> reportComponents() {
                return Collections.emptyList();
            }

            @Override
            public List<AddDatatype> datatypes() {
                return Collections.emptyList();
            }

            @Override
            public List<GoogleStorageLocation> failedLogLocations() {
                return Collections.emptyList();
            }
        };
    }

}