package com.hartwig.pipeline.stages;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.CopyWriter;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.metadata.RunMetadata;
import com.hartwig.pipeline.storage.RuntimeBucket;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class StageTest<S extends StageOutput, M extends RunMetadata> {

    protected Storage storage;
    protected RuntimeBucket runtimeBucket;
    protected Stage<S, M> victim;
    protected Bucket bucket;

    @Before
    public void setUp() throws Exception {
        victim = createVictim();
        String runtimeBucketName = expectedRuntimeBucketName() + "/" + victim.namespace();
        storage = mock(Storage.class);
        bucket = mock(Bucket.class);
        when(storage.get(runtimeBucketName)).thenReturn(bucket);
        CopyWriter copyWriter = mock(CopyWriter.class);
        when(storage.copy(any())).thenReturn(copyWriter);
        storage = mock(Storage.class);
        runtimeBucket = mock(RuntimeBucket.class);
        when(runtimeBucket.name()).thenReturn(runtimeBucketName);
    }

    @Test
    public void declaresExpectedInputs() {
        assertThat(victim.inputs().stream().map(BashCommand::asBash).collect(Collectors.toList())).isEqualTo(expectedInputs());
    }

    @Test
    public void declaredExpectedCommands() {
        assertThat(victim.commands(input()).stream().map(BashCommand::asBash).collect(Collectors.toList())).isEqualTo(commands(
                expectedCommands()));
    }

    @Test
    public void disabledAppropriately() {
        assertThat(victim.shouldRun(createDisabledArguments())).isFalse();
    }

    @Test
    public void enabledAppropriately() {
        assertThat(victim.shouldRun(defaultArguments())).isTrue();
    }

    @Test
    public void returnsExpectedOutput() {
        S output = victim.output(input(), PipelineStatus.SUCCESS, runtimeBucket, ResultsDirectory.defaultDirectory());
        assertThat(output.status()).isEqualTo(PipelineStatus.SUCCESS);
        assertThat(output.name()).isEqualTo(victim.namespace());
        validateOutput(output);
    }

    private Arguments defaultArguments() {
        return Arguments.testDefaults();
    }

    protected abstract Arguments createDisabledArguments();

    protected abstract Stage<S, M> createVictim();

    protected abstract M input();

    protected abstract List<String> expectedInputs();

    protected abstract String expectedRuntimeBucketName();

    protected abstract List<String> expectedCommands();

    protected abstract void validateOutput(final S output);

    protected static String input(String source, String target) {
        return String.format("gsutil -qm cp -r -n gs://%s /data/input/%s", source, target);
    }

    private List<String> commands(List<String> commands) {
        return commands.stream().map(command -> command.replace("$TOOLS_DIR", VmDirectories.TOOLS)).collect(Collectors.toList());
    }
}