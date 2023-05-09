package com.hartwig.pipeline.output;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.common.collect.Lists;
import com.hartwig.pipeline.PipelineState;
import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.output.AddDatatype;
import com.hartwig.pipeline.output.ComposeInPipelineOutputBucket;
import com.hartwig.pipeline.output.Folder;
import com.hartwig.pipeline.output.OutputComponent;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class ComposeInPipelineOutputBucketTest {

    private static final String PIPELINE_OUTPUT = "pipeline-output";
    private boolean firstComponentRan;
    private boolean secondComponentRan;
    private ComposeInPipelineOutputBucket victim;
    private Bucket outputBucket;
    private Storage storage;

    @Before
    public void setUp() throws Exception {
        storage = mock(Storage.class);
        outputBucket = mock(Bucket.class);
        when(outputBucket.getName()).thenReturn(PIPELINE_OUTPUT);
        victim = new ComposeInPipelineOutputBucket("test", storage, outputBucket);
    }

    @Test
    public void composesAllAddedComponents() {
        victim.add(stageOutput(Lists.newArrayList((s, r, setName) -> firstComponentRan = true,
                (s, r, setName) -> secondComponentRan = true)));
        victim.compose(TestInputs.referenceRunMetadata(), Folder.root());
        assertThat(firstComponentRan).isTrue();
        assertThat(secondComponentRan).isTrue();
    }

    @Test(expected = RuntimeException.class)
    public void failsWhenOneComponentFails() {
        victim.add(stageOutput(Lists.newArrayList((s, r, setName) -> firstComponentRan = true, (s, r, setName) -> {
            throw new RuntimeException();
        }, (s, r, setName) -> secondComponentRan = true)));
        victim.compose(TestInputs.referenceRunMetadata(),  Folder.root());
    }

    @Test
    public void copiesMetadataRunVersionAndCompletionToRootOfBucketSomatic() {
        ArgumentCaptor<String> createBlobCaptor = ArgumentCaptor.forClass(String.class);
        victim.compose(TestInputs.defaultSomaticRunMetadata(), Folder.root());
        verify(outputBucket, times(2)).create(createBlobCaptor.capture(), (byte[]) any());
        assertThat(createBlobCaptor.getAllValues().get(0)).isEqualTo("set/metadata.json");
        assertThat(createBlobCaptor.getAllValues().get(1)).isEqualTo("set/pipeline.version");
    }

    @NotNull
    public PipelineState success() {
        return new PipelineState();
    }

    @NotNull
    private StageOutput stageOutput(final ArrayList<OutputComponent> components) {
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
            public List<OutputComponent> reportComponents() {
                return components;
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