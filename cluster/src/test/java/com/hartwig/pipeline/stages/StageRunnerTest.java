package com.hartwig.pipeline.stages;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.computeengine.execution.ComputeEngineStatus;
import com.hartwig.computeengine.execution.vm.ComputeEngine;
import com.hartwig.computeengine.execution.vm.command.BashCommand;
import com.hartwig.computeengine.execution.vm.command.unix.ExportVariableCommand;
import com.hartwig.computeengine.storage.ResultsDirectory;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.input.InputMode;
import com.hartwig.pipeline.input.SomaticRunMetadata;
import com.hartwig.pipeline.labels.Labels;
import com.hartwig.pipeline.reruns.StartingPoint;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Before;
import org.junit.Test;

public class StageRunnerTest {

    private static final SomaticRunMetadata METADATA = TestInputs.defaultSomaticRunMetadata();
    private static final Arguments ARGUMENTS = Arguments.testDefaults();
    private static final String NAMESPACE = "test";
    private StageRunner<SomaticRunMetadata> victim;
    private Stage<StageOutput, SomaticRunMetadata> stage;
    private StageOutput output;
    private StartingPoint startingPoint;
    private Storage storage;
    private ComputeEngine computeEngine;

    @Before
    public void setUp() throws Exception {
        storage = mock(Storage.class);
        Bucket runtimeBucket = mock(Bucket.class);
        when(runtimeBucket.getName()).thenReturn("run-1-test");
        when(storage.get("run-1-test")).thenReturn(runtimeBucket);
        computeEngine = mock(ComputeEngine.class);
        when(computeEngine.submit(any(), any())).thenReturn(ComputeEngineStatus.SUCCESS);
        //noinspection unchecked
        stage = mock(Stage.class);
        startingPoint = mock(StartingPoint.class);
        victim = new StageRunner<>(storage,
                ARGUMENTS,
                computeEngine,
                ResultsDirectory.defaultDirectory(),
                startingPoint,
                Labels.of(ARGUMENTS),
                InputMode.TUMOR_REFERENCE);
        when(stage.namespace()).thenReturn(NAMESPACE);
        output = mock(StageOutput.class);
        when(stage.output(eq(METADATA), any(), any(), any())).thenReturn(output);
    }

    @Test
    public void runsStageAndReturnsOutputTumorReferenceMode() {
        when(startingPoint.usePersisted(NAMESPACE)).thenReturn(false);
        when(stage.shouldRun(ARGUMENTS)).thenReturn(true);
        when(stage.tumorReferenceCommands(METADATA)).thenReturn(simpleBash());
        assertThat(victim.run(METADATA, stage)).isEqualTo(output);
    }

    @Test
    public void runsStageAndReturnsOutputReferenceOnlyMode() {
        victim = new StageRunner<>(storage,
                ARGUMENTS,
                computeEngine,
                ResultsDirectory.defaultDirectory(),
                startingPoint,
                Labels.of(ARGUMENTS),
                InputMode.REFERENCE_ONLY);
        when(startingPoint.usePersisted(NAMESPACE)).thenReturn(false);
        when(stage.shouldRun(ARGUMENTS)).thenReturn(true);
        when(stage.referenceOnlyCommands(METADATA)).thenReturn(simpleBash());
        assertThat(victim.run(METADATA, stage)).isEqualTo(output);
    }

    @Test
    public void runsStageAndReturnsOutputTumorOnlyMode() {
        victim = new StageRunner<>(storage,
                ARGUMENTS,
                computeEngine,
                ResultsDirectory.defaultDirectory(),
                startingPoint,
                Labels.of(ARGUMENTS),
                InputMode.TUMOR_ONLY);
        when(startingPoint.usePersisted(NAMESPACE)).thenReturn(false);
        when(stage.shouldRun(ARGUMENTS)).thenReturn(true);
        when(stage.tumorOnlyCommands(METADATA)).thenReturn(simpleBash());
        assertThat(victim.run(METADATA, stage)).isEqualTo(output);
    }

    @Test
    public void skipsStageWhenDisabled() {
        when(startingPoint.usePersisted(NAMESPACE)).thenReturn(false);
        final StageOutput skippedOutput = mock(StageOutput.class);
        when(stage.skippedOutput(METADATA)).thenReturn(skippedOutput);
        when(stage.shouldRun(ARGUMENTS)).thenReturn(false);
        assertThat(victim.run(METADATA, stage)).isEqualTo(skippedOutput);
    }

    @Test
    public void usesPersistedOutputWhenEarlierThanStartingPoint() {
        when(startingPoint.usePersisted(NAMESPACE)).thenReturn(true);
        final StageOutput persistedOutput = mock(StageOutput.class);
        when(stage.persistedOutput(METADATA)).thenReturn(persistedOutput);
        when(stage.shouldRun(ARGUMENTS)).thenReturn(true);
        when(stage.tumorReferenceCommands(METADATA)).thenReturn(simpleBash());
        assertThat(victim.run(METADATA, stage)).isEqualTo(persistedOutput);
    }

    private static List<BashCommand> simpleBash() {
        return List.of(new ExportVariableCommand("test", "test"));
    }
}