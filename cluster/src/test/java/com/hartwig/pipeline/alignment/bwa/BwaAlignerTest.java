package com.hartwig.pipeline.alignment.bwa;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.Executors;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.CopyWriter;
import com.google.cloud.storage.Storage;
import com.hartwig.pdl.LaneInput;
import com.hartwig.pdl.PipelineInput;
import com.hartwig.pdl.SampleInput;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.alignment.AlignmentOutput;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.GoogleComputeEngine;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.input.SingleSampleRunMetadata;
import com.hartwig.pipeline.labels.Labels;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.storage.SampleUpload;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class BwaAlignerTest {

    private static final SingleSampleRunMetadata METADATA = TestInputs.referenceRunMetadata();
    private BwaAligner victim;
    private SampleUpload sampleUpload;
    private Storage storage;
    private GoogleComputeEngine computeEngine;
    private Arguments arguments;

    @Before
    public void setUp() throws Exception {
        arguments = Arguments.testDefaults();
        computeEngine = mock(GoogleComputeEngine.class);
        storage = mock(Storage.class);
        sampleUpload = mock(SampleUpload.class);
        PipelineInput input = PipelineInput.builder()
                .setName(TestInputs.SET)
                .reference(SampleInput.builder().name(METADATA.sampleName()).addLanes(lane(1)).addLanes(lane(2)).build())
                .build();
        victim = new BwaAligner(arguments,
                computeEngine,
                storage,
                input,
                sampleUpload,
                ResultsDirectory.defaultDirectory(),
                Executors.newSingleThreadExecutor(),
                mock(Labels.class));
    }

    @Test
    public void launchesComputeEngineJobForEachLane() throws Exception {
        setupMocks();

        ArgumentCaptor<RuntimeBucket> bucketCaptor = ArgumentCaptor.forClass(RuntimeBucket.class);
        ArgumentCaptor<VirtualMachineJobDefinition> jobDefinitionArgumentCaptor =
                ArgumentCaptor.forClass(VirtualMachineJobDefinition.class);
        when(computeEngine.submit(bucketCaptor.capture(), jobDefinitionArgumentCaptor.capture())).thenReturn(PipelineStatus.SUCCESS);
        victim.run(METADATA);
        assertThat(bucketCaptor.getAllValues().get(0).name()).isEqualTo("run-reference-test/aligner/flowcell-L001");
        assertThat(bucketCaptor.getAllValues().get(1).name()).isEqualTo("run-reference-test/aligner/flowcell-L002");

        assertThat(jobDefinitionArgumentCaptor.getAllValues().get(0).name()).isEqualTo("aligner-flowcell-l001");
        assertThat(jobDefinitionArgumentCaptor.getAllValues().get(1).name()).isEqualTo("aligner-flowcell-l002");
    }

    @Test
    public void failsWhenAnyLaneFails() throws Exception {
        setupMocks();
        when(computeEngine.submit(any(), any())).thenReturn(PipelineStatus.SUCCESS);
        when(computeEngine.submit(any(), argThat(jobDef -> jobDef.name().contains("l001")))).thenReturn(PipelineStatus.FAILED);
        assertThat(victim.run(METADATA).status()).isEqualTo(PipelineStatus.FAILED);
    }

    @Test
    public void mergesAllLanesIntoOneComputeEngineJob() throws Exception {
        setupMocks();
        ArgumentCaptor<RuntimeBucket> bucketCaptor = ArgumentCaptor.forClass(RuntimeBucket.class);
        ArgumentCaptor<VirtualMachineJobDefinition> jobDefinitionArgumentCaptor =
                ArgumentCaptor.forClass(VirtualMachineJobDefinition.class);
        when(computeEngine.submit(bucketCaptor.capture(), jobDefinitionArgumentCaptor.capture())).thenReturn(PipelineStatus.SUCCESS);
        victim.run(METADATA);
        assertThat(bucketCaptor.getAllValues().get(2).name()).isEqualTo("run-reference-test/aligner");
        assertThat(jobDefinitionArgumentCaptor.getAllValues().get(2).name()).isEqualTo("merge-markdup");
    }

    @Test
    public void returnsProvidedBamIfInSample() throws Exception {
        String gsUrl = "gs://bucket/path/reference.bam";
        PipelineInput input = PipelineInput.builder()
                .setName(METADATA.set())
                .reference(SampleInput.builder().name(METADATA.sampleName()).bam(gsUrl).build())
                .build();
        victim = new BwaAligner(arguments,
                computeEngine,
                storage,
                input,
                sampleUpload,
                ResultsDirectory.defaultDirectory(),
                Executors.newSingleThreadExecutor(),
                mock(Labels.class));
        AlignmentOutput output = victim.run(METADATA);
        assertThat(output.alignments()).isEqualTo(GoogleStorageLocation.from(gsUrl, arguments.project()));
        assertThat(output.sample()).isEqualTo(METADATA.sampleName());
        assertThat(output.status()).isEqualTo(PipelineStatus.PROVIDED);
    }

    private void setupMocks() {
        CopyWriter copyWriter = mock(CopyWriter.class);
        when(storage.copy(any())).thenReturn(copyWriter);
        String rootBucketName = "run-" + METADATA.sampleName().toLowerCase() + "-test";
        Bucket rootBucket = mock(Bucket.class);
        when(rootBucket.getName()).thenReturn(rootBucketName);
        when(storage.get(rootBucketName)).thenReturn(rootBucket);
    }

    private static LaneInput lane(final int index) {
        return Lanes.emptyBuilder().flowCellId("flowcell").laneNumber(String.format("L00%s", index)).build();
    }
}