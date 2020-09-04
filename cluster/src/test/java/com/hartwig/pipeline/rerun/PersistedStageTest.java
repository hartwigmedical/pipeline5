package com.hartwig.pipeline.rerun;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.execution.vm.ComputeEngine;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.stages.StageRunner;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Test;

public class PersistedStageTest {

    @Test
    public void usesPersistedOutputIfBeforeStartingPoint() {
        final StageOutput expected = mock(StageOutput.class);
        StartingPoint startingPoint = mock(StartingPoint.class);
        when(startingPoint.usePersisted(anyString())).thenReturn(true);
        @SuppressWarnings("unchecked")
        PersistedStage<StageOutput, SomaticRunMetadata> victim =
                new PersistedStage<StageOutput, SomaticRunMetadata>(mock(Stage.class), "bucket", startingPoint, "set") {
                    @Override
                    public StageOutput skippedOutput(final SomaticRunMetadata metadata) {
                        return expected;
                    }
                };
        StageRunner<SomaticRunMetadata> stageRunner = new StageRunner<>(mock(Storage.class),
                Arguments.testDefaults(),
                mock(ComputeEngine.class),
                ResultsDirectory.defaultDirectory());
        StageOutput results = stageRunner.run(TestInputs.defaultSomaticRunMetadata(), victim);
        assertThat(results).isEqualTo(expected);
    }
}