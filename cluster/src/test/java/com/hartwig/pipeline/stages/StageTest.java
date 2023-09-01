package com.hartwig.pipeline.stages;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.CopyWriter;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.PipelineStatus;
import com.hartwig.computeengine.execution.vm.VmDirectories;
import com.hartwig.computeengine.execution.vm.command.BashCommand;
import com.hartwig.pipeline.input.RunMetadata;
import com.hartwig.computeengine.storage.ResultsDirectory;
import com.hartwig.computeengine.storage.RuntimeBucket;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.output.AddDatatype;
import com.hartwig.pipeline.testsupport.TestInputs;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.hartwig.pipeline.testsupport.TestInputs.inputDownload;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class StageTest<S extends StageOutput, M extends RunMetadata> {

    private static final Logger LOGGER = LoggerFactory.getLogger(StageTest.class);
    protected static final String OUTPUT_BUCKET = TestInputs.BUCKET;

    protected Storage storage;
    protected RuntimeBucket runtimeBucket;
    protected Stage<S, M> victim;
    protected Bucket bucket;
    protected TestPersistedDataset persistedDataset;

    @Before
    public void setUp() throws Exception {
        persistedDataset = new TestPersistedDataset();
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
        when(runtimeBucket.get(any())).thenReturn(mock(Blob.class));
    }

    @Test
    public void declaresExpectedInputs() {
        assertThat(victim.inputs()
                .stream()
                .map(BashCommand::asBash)
                .collect(Collectors.toList())).containsExactlyInAnyOrder(expectedInputs().toArray(String[]::new));
    }

    @Test
    public void declaredExpectedCommands() {
        assertThat(victim.tumorReferenceCommands(input())
                .stream()
                .map(BashCommand::asBash)
                .collect(Collectors.toList())).isEqualTo(commands(expectedCommands()));
    }

    @Test
    public void declaredExpectedGermlineOnlyCommands() {
        if (!expectedReferenceOnlyCommands().isEmpty()) {
            assertThat(victim.referenceOnlyCommands(input()).stream().map(BashCommand::asBash).collect(Collectors.toList())).isEqualTo(
                    commands(expectedReferenceOnlyCommands()));
        }
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
    public void enabledOnShallowSeq() {
        assertThat(victim.shouldRun(Arguments.builder().from(defaultArguments()).shallow(true).build())).isEqualTo(isEnabledOnShallowSeq());
    }

    @Test
    public void returnsExpectedOutput() {
        S output = victim.output(input(), PipelineStatus.SUCCESS, runtimeBucket, ResultsDirectory.defaultDirectory());
        assertThat(output.status()).isEqualTo(PipelineStatus.SUCCESS);
        assertThat(output.name()).isEqualTo(victim.namespace());
        validateOutput(output);
    }

    @Test
    public void returnsExpectedPersistedOutput() {
        try {
            S output = victim.persistedOutput(input());
            validatePersistedOutput(output);
        } catch (UnsupportedOperationException e) {
            LOGGER.info("Persisted output not supported for stage [{}]. No test required", victim.namespace());
        }
    }

    @Test
    public void returnsExpectedPersistedOutputFromPersistedDataset() {
        try {
            setupPersistedDataset();
            S output = victim.persistedOutput(input());
            validatePersistedOutputFromPersistedDataset(output);
        } catch (UnsupportedOperationException e) {
            LOGGER.info("Persisted output not supported for stage [{}]. No test required", victim.namespace());
        }
    }

    @Test
    public void returnsExpectedFurtherOperations() {
        assertThat(victim.output(input(), PipelineStatus.SUCCESS, runtimeBucket, ResultsDirectory.defaultDirectory())
                .datatypes()).containsExactlyInAnyOrder(expectedFurtherOperations().toArray(AddDatatype[]::new));
    }

    @Test
    public void addsLogs() {
        assertThat(victim.output(input(), PipelineStatus.SUCCESS, runtimeBucket, ResultsDirectory.defaultDirectory())
                .failedLogLocations()).isNotEmpty();
    }

    protected List<AddDatatype> expectedFurtherOperations() {
        return Collections.emptyList();
    }

    protected void setupPersistedDataset() {
        // do nothing by default
    }

    protected void validatePersistedOutputFromPersistedDataset(final S output) {
        validatePersistedOutput(output);
    }

    protected void validatePersistedOutput(final S output) {
        fail("This class implements persisted output. Validate it by overriding this method!");
    }

    protected Arguments defaultArguments() {
        return Arguments.testDefaults();
    }

    protected abstract Arguments createDisabledArguments();

    protected abstract Stage<S, M> createVictim();

    protected abstract M input();

    protected abstract List<String> expectedInputs();

    protected abstract String expectedRuntimeBucketName();

    protected abstract List<String> expectedCommands();

    protected List<String> expectedReferenceOnlyCommands() {
        return Collections.emptyList();
    }

    protected abstract void validateOutput(final S output);

    protected static String input(final String source, final String target) {
        return inputDownload(format("cp -r -n gs://%s /data/input/%s", source, target));
    }

    private List<String> commands(final List<String> commands) {
        return commands.stream().map(command -> command.replace("$TOOLS_DIR", VmDirectories.TOOLS)).collect(Collectors.toList());
    }

    protected boolean isEnabledOnShallowSeq() {
        return true;
    }
}