package com.hartwig.pipeline.stages;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.CopyWriter;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.ResourceDownload;
import com.hartwig.pipeline.metadata.RunMetadata;
import com.hartwig.pipeline.storage.RuntimeBucket;

import org.junit.Before;
import org.junit.Test;

public abstract class StageTest<S extends StageOutput, M extends RunMetadata> {

    protected Storage storage;
    protected RuntimeBucket runtimeBucket;
    protected Stage<S, M> victim;

    @Before
    public void setUp() throws Exception {
        victim = createVictim();
        String runtimeBucketName = expectedRuntimeBucketName() + "/" + victim.namespace();
        storage = mock(Storage.class);
        final Bucket bucket = mock(Bucket.class);
        when(storage.get(runtimeBucketName)).thenReturn(bucket);
        CopyWriter copyWriter = mock(CopyWriter.class);
        when(storage.copy(any())).thenReturn(copyWriter);
        storage = mock(Storage.class);
        runtimeBucket = mock(RuntimeBucket.class);
        when(runtimeBucket.name()).thenReturn(runtimeBucketName);
    }

    @Test
    public void declaresExpectedInputs() {
        assertThat(victim.inputs().stream().map(InputDownload::asBash).collect(Collectors.toList())).isEqualTo(expectedInputs());
    }

    @Test
    public void declaresExpectedResources() {
        assertThat(victim.resources(storage, defaultArguments().resourceBucket(), runtimeBucket)
                .stream()
                .map(ResourceDownload::asBash)
                .collect(Collectors.toList())).isEqualTo(expectedResources());
    }

    @Test
    public void declaredExpectedCommands() {
        Map<String, ResourceDownload> resourceMap =
                StageRunner.resourceMap(victim.resources(storage, defaultArguments().resourceBucket(), runtimeBucket));
        assertThat(victim.commands(input(), resourceMap).stream().map(BashCommand::asBash).collect(Collectors.toList())).isEqualTo(
                expectedCommands());
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
        assertThat(validateOutput(victim.output(input(),
                PipelineStatus.SUCCESS,
                runtimeBucket,
                ResultsDirectory.defaultDirectory()))).isTrue();
    }

    private Arguments defaultArguments() {
        return Arguments.testDefaults();
    }

    protected abstract Arguments createDisabledArguments();

    protected abstract Stage<S, M> createVictim();

    protected abstract M input();

    protected abstract List<String> expectedInputs();

    protected abstract List<String> expectedResources();

    protected abstract String expectedRuntimeBucketName();

    protected abstract List<String> expectedCommands();

    protected abstract boolean validateOutput(final S output);

    protected static String input(String source, String target) {
        return String.format("gsutil -qm cp -n gs://%s /data/input/%s", source, target);
    }

    protected String resource(String resourceName) {
        return String.format("gsutil -qm cp gs://%s/%s/%s/* /data/resources",
                expectedRuntimeBucketName(),
                victim.namespace(),
                resourceName);
    }
}