package com.hartwig.pipeline.alignment.bwa;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.Executors;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.CopyWriter;
import com.google.cloud.storage.Storage;
import com.hartwig.computeengine.execution.ComputeEngineStatus;
import com.hartwig.computeengine.execution.vm.GoogleComputeEngine;
import com.hartwig.computeengine.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.computeengine.storage.GoogleStorageLocation;
import com.hartwig.computeengine.storage.ResultsDirectory;
import com.hartwig.computeengine.storage.RuntimeBucket;
import com.hartwig.pdl.LaneInput;
import com.hartwig.pdl.PipelineInput;
import com.hartwig.pdl.SampleInput;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.PipelineStatus;
import com.hartwig.pipeline.alignment.AlignmentOutput;
import com.hartwig.pipeline.input.SingleSampleRunMetadata;
import com.hartwig.pipeline.labels.Labels;
import com.hartwig.pipeline.storage.SampleUpload;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class BwaAlignerTest {

    private static final SingleSampleRunMetadata METADATA = TestInputs.referenceRunMetadata();
    public static final String GS_BUCKET_PATH_REFERENCE_BAM = "gs://bucket/path/reference.bam";
    public static final String GS_BUCKET_PATH_REFERENCE_CRAM = "gs://bucket/path/reference.cram";
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
        victim = createVictimBwaAligner(input);
    }

    @Test
    public void launchesComputeEngineJobForEachLane() throws Exception {
        setupMocks();

        ArgumentCaptor<RuntimeBucket> bucketCaptor = ArgumentCaptor.forClass(RuntimeBucket.class);
        ArgumentCaptor<VirtualMachineJobDefinition> jobDefinitionArgumentCaptor =
                ArgumentCaptor.forClass(VirtualMachineJobDefinition.class);
        when(computeEngine.submit(bucketCaptor.capture(), jobDefinitionArgumentCaptor.capture())).thenReturn(ComputeEngineStatus.SUCCESS);
        victim.run(METADATA);
        assertThat(bucketCaptor.getAllValues().get(0).name()).isEqualTo(TestInputs.REFERENCE_BUCKET + "/aligner/flowcell-L001");
        assertThat(bucketCaptor.getAllValues().get(1).name()).isEqualTo(TestInputs.REFERENCE_BUCKET + "/aligner/flowcell-L002");

        assertThat(jobDefinitionArgumentCaptor.getAllValues().get(0).name()).isEqualTo("aligner-flowcell-l001");
        assertThat(jobDefinitionArgumentCaptor.getAllValues().get(1).name()).isEqualTo("aligner-flowcell-l002");
    }

    @Test
    public void skipLaneAlignmentIfRedoDuplicatesFromBam() throws Exception {
        arguments = Arguments.testDefaultsBuilder().redoDuplicateMarking(true).build();
        PipelineInput input = createBamPipelineInput();
        victim = createVictimBwaAligner(input);

        setupMocks();
        ArgumentCaptor<RuntimeBucket> bucketCaptor = ArgumentCaptor.forClass(RuntimeBucket.class);
        ArgumentCaptor<VirtualMachineJobDefinition> jobDefinitionArgumentCaptor =
                ArgumentCaptor.forClass(VirtualMachineJobDefinition.class);
        when(computeEngine.submit(bucketCaptor.capture(), jobDefinitionArgumentCaptor.capture())).thenReturn(ComputeEngineStatus.SUCCESS);
        victim.run(METADATA);

        assertThat(bucketCaptor.getAllValues()
                .stream()
                .noneMatch(b -> b.name().equals(TestInputs.REFERENCE_BUCKET + "/aligner/flowcell-L001"))).isTrue();
        assertThat(jobDefinitionArgumentCaptor.getAllValues().size()).isEqualTo(1);
        assertThat(jobDefinitionArgumentCaptor.getAllValues().get(0).name()).isEqualTo("merge-redux");
    }

    @Test
    public void skipLaneAlignmentIfRedoDuplicatesFromCram() throws Exception {
        arguments = Arguments.testDefaultsBuilder().redoDuplicateMarking(true).build();
        PipelineInput input = createCramPipelineInput();
        victim = createVictimBwaAligner(input);

        setupMocks();
        ArgumentCaptor<RuntimeBucket> bucketCaptor = ArgumentCaptor.forClass(RuntimeBucket.class);
        ArgumentCaptor<VirtualMachineJobDefinition> jobDefinitionArgumentCaptor =
                ArgumentCaptor.forClass(VirtualMachineJobDefinition.class);
        when(computeEngine.submit(bucketCaptor.capture(), jobDefinitionArgumentCaptor.capture())).thenReturn(ComputeEngineStatus.SUCCESS);
        victim.run(METADATA);

        assertThat(bucketCaptor.getAllValues()
                .stream()
                .noneMatch(b -> b.name().equals(TestInputs.REFERENCE_BUCKET + "/aligner/flowcell-L001"))).isTrue();
        assertThat(jobDefinitionArgumentCaptor.getAllValues().size()).isEqualTo(1);
        assertThat(jobDefinitionArgumentCaptor.getAllValues().get(0).name()).isEqualTo("merge-redux");
    }

    @Test
    public void outputBamIfRedoDuplicatesFromCram() throws Exception {
        arguments = Arguments.testDefaultsBuilder().redoDuplicateMarking(true).build();
        PipelineInput input = createCramPipelineInput();
        victim = createVictimBwaAligner(input);
        setupMocks();
        when(computeEngine.submit(any(), any())).thenReturn(ComputeEngineStatus.SUCCESS);
        AlignmentOutput output = victim.run(METADATA);
        assertThat(output.alignments().path()).endsWith(".bam");
    }

    @Test
    public void failsWhenAnyLaneFails() throws Exception {
        setupMocks();
        when(computeEngine.submit(any(), any())).thenReturn(ComputeEngineStatus.SUCCESS);
        when(computeEngine.submit(any(), argThat(jobDef -> jobDef.name().contains("l001")))).thenReturn(ComputeEngineStatus.FAILED);
        assertThat(victim.run(METADATA).status()).isEqualTo(PipelineStatus.FAILED);
    }

    @Test
    public void mergesAllLanesIntoOneComputeEngineJob() throws Exception {
        setupMocks();
        ArgumentCaptor<RuntimeBucket> bucketCaptor = ArgumentCaptor.forClass(RuntimeBucket.class);
        ArgumentCaptor<VirtualMachineJobDefinition> jobDefinitionArgumentCaptor =
                ArgumentCaptor.forClass(VirtualMachineJobDefinition.class);
        when(computeEngine.submit(bucketCaptor.capture(), jobDefinitionArgumentCaptor.capture())).thenReturn(ComputeEngineStatus.SUCCESS);
        victim.run(METADATA);
        assertThat(bucketCaptor.getAllValues().get(2).name()).isEqualTo(TestInputs.REFERENCE_BUCKET + "/aligner");
        assertThat(jobDefinitionArgumentCaptor.getAllValues().get(2).name()).isEqualTo("merge-redux");
    }

    @Test
    public void returnsProvidedBamIfInSampleAndNotRedoDuplicateMarking() throws Exception {
        setupMocks();
        arguments = Arguments.testDefaultsBuilder().redoDuplicateMarking(false).build();
        PipelineInput input = createBamPipelineInput();
        victim = createVictimBwaAligner(input);
        AlignmentOutput output = victim.run(METADATA);
        assertThat(output.alignments()).isEqualTo(GoogleStorageLocation.from(GS_BUCKET_PATH_REFERENCE_BAM, arguments.project()));
        assertThat(output.sample()).isEqualTo(METADATA.sampleName());
        assertThat(output.status()).isEqualTo(PipelineStatus.PROVIDED);
    }

    @Test
    public void returnsProvidedCramIfInSampleAndNotRedoDuplicateMarking() throws Exception {
        setupMocks();
        arguments = Arguments.testDefaultsBuilder().redoDuplicateMarking(false).build();
        PipelineInput input = createCramPipelineInput();
        victim = createVictimBwaAligner(input);
        AlignmentOutput output = victim.run(METADATA);
        assertThat(output.alignments()).isEqualTo(GoogleStorageLocation.from(GS_BUCKET_PATH_REFERENCE_CRAM, arguments.project()));
        assertThat(output.sample()).isEqualTo(METADATA.sampleName());
        assertThat(output.status()).isEqualTo(PipelineStatus.PROVIDED);
    }

    private BwaAligner createVictimBwaAligner(final PipelineInput input) {
        return new BwaAligner(arguments,
                computeEngine,
                storage,
                input,
                sampleUpload,
                ResultsDirectory.defaultDirectory(),
                Executors.newSingleThreadExecutor(),
                mock(Labels.class));
    }

    private static PipelineInput createBamPipelineInput() {
        return PipelineInput.builder()
                .setName(METADATA.set())
                .reference(SampleInput.builder().name(METADATA.sampleName()).bam(GS_BUCKET_PATH_REFERENCE_BAM).build())
                .build();
    }

    private static PipelineInput createCramPipelineInput() {
        return PipelineInput.builder()
                .setName(METADATA.set())
                .reference(SampleInput.builder().name(METADATA.sampleName()).bam(GS_BUCKET_PATH_REFERENCE_CRAM).build())
                .build();
    }

    private void setupMocks() {
        CopyWriter copyWriter = mock(CopyWriter.class);
        when(storage.copy(any())).thenReturn(copyWriter);
        String rootBucketName = TestInputs.REFERENCE_BUCKET;
        Bucket rootBucket = mock(Bucket.class);
        when(rootBucket.getName()).thenReturn(rootBucketName);
        when(storage.get(rootBucketName)).thenReturn(rootBucket);
        when(storage.get(any(BlobId.class), any(Storage.BlobGetOption[].class))).thenReturn(mock(Blob.class));
    }

    private static LaneInput lane(final int index) {
        return Lanes.emptyBuilder().flowCellId("flowcell").laneNumber(String.format("L00%s", index)).build();
    }
}
