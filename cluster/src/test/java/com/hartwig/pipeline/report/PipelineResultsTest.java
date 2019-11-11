package com.hartwig.pipeline.report;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.common.collect.Lists;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.PipelineState;
import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.metrics.BamMetricsOutput;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class PipelineResultsTest {

    private static final String PIPELINE_OUTPUT = "pipeline-output";
    private boolean firstComponentRan;
    private boolean secondComponentRan;
    private PipelineResults victim;
    private Bucket outputBucket;

    @Before
    public void setUp() throws Exception {
        final Storage storage = mock(Storage.class);
        outputBucket = mock(Bucket.class);
        when(outputBucket.getName()).thenReturn(PIPELINE_OUTPUT);
        victim = new PipelineResults("test", storage, outputBucket, Arguments.testDefaultsBuilder().runId("tag").build());
    }

    @Test
    public void composesAllAddedComponents() {
        victim.add(stageOutput(Lists.newArrayList((s, r, setName) -> firstComponentRan = true,
                (s, r, setName) -> secondComponentRan = true)));
        victim.compose(TestInputs.referenceRunMetadata(), success());
        assertThat(firstComponentRan).isTrue();
        assertThat(secondComponentRan).isTrue();
    }

    @Test(expected = RuntimeException.class)
    public void failsHardWhenOneComponentFails() {
        victim.add(stageOutput(Lists.newArrayList((s, r, setName) -> firstComponentRan = true, (s, r, setName) -> {
            throw new RuntimeException();
        }, (s, r, setName) -> secondComponentRan = true)));
        victim.compose(TestInputs.referenceRunMetadata(), success());
    }

    @Test
    public void copiesMetadataRunVersionAndCompletionToRootOfBucketSingleSample() {
        ArgumentCaptor<String> createBlobCaptor = ArgumentCaptor.forClass(String.class);
        victim.compose(TestInputs.referenceRunMetadata(), success());
        verify(outputBucket, times(3)).create(createBlobCaptor.capture(), (byte[]) any());
        assertThat(createBlobCaptor.getAllValues().get(0)).isEqualTo("reference-tag/reference/metadata.json");
        assertThat(createBlobCaptor.getAllValues().get(1)).isEqualTo("reference-tag/reference/pipeline.version");
        assertThat(createBlobCaptor.getAllValues().get(2)).isEqualTo("reference-tag/STAGED");
    }

    @Test
    public void copiesMetadataRunVersionAndCompletionToRootOfBucketSomatic() {
        ArgumentCaptor<String> createBlobCaptor = ArgumentCaptor.forClass(String.class);
        victim.compose(TestInputs.defaultSomaticRunMetadata());
        verify(outputBucket, times(3)).create(createBlobCaptor.capture(), (byte[]) any());
        assertThat(createBlobCaptor.getAllValues().get(0)).isEqualTo("run/metadata.json");
        assertThat(createBlobCaptor.getAllValues().get(1)).isEqualTo("run/pipeline.version");
        assertThat(createBlobCaptor.getAllValues().get(2)).isEqualTo("run/STAGED");
    }

    @Test
    public void onlyWritesStagedFileWhenPipelineFailsSingleSample() {
        ArgumentCaptor<String> createBlobCaptor = ArgumentCaptor.forClass(String.class);
        PipelineState state = new PipelineState();
        state.add(BamMetricsOutput.builder().sample("reference").status(PipelineStatus.FAILED).build());
        victim.compose(TestInputs.referenceRunMetadata(), state);
        verify(outputBucket, times(1)).create(createBlobCaptor.capture(), (byte[]) any());
        assertThat(createBlobCaptor.getAllValues().get(0)).isEqualTo("reference-tag/STAGED");
    }

    @NotNull
    public PipelineState success() {
        return new PipelineState();
    }

    @NotNull
    private StageOutput stageOutput(final ArrayList<ReportComponent> components) {
        return new StageOutput() {
            @Override
            public String name() {
                return "test";
            }

            @Override
            public PipelineStatus status() {
                return PipelineStatus.SUCCESS;
            }

            @Override
            public List<ReportComponent> reportComponents() {
                return components;
            }
        };
    }
}