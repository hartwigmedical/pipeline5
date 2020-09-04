package com.hartwig.pipeline.rerun;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.metadata.RunMetadata;
import com.hartwig.pipeline.stages.Stage;

import org.junit.Test;

public abstract class AbstractPersistedStageTest<T extends StageOutput, M extends RunMetadata> {

    static final String OUTPUT_BUCKET = Arguments.testDefaults().outputBucket();

    @Test
    public void returnsPersistedOutput() {
        @SuppressWarnings("unchecked")
        Stage<T, M> decorated = mock(Stage.class);
        when(decorated.namespace()).thenReturn(namespace());
        PersistedStage<T, M> victim = createStage(decorated, Arguments.testDefaults(), "set");
        T output = victim.skippedOutput(metadata());
        assertThat(output.status()).isEqualTo(PipelineStatus.PERSISTED);
        assertOutput(output);
    }

    protected abstract void assertOutput(final T output);

    protected abstract M metadata();

    protected abstract PersistedStage<T, M> createStage(final Stage<T, M> decorated, final Arguments testDefaults, final String set);

    protected abstract String namespace();
}
