package com.hartwig.pipeline.input;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.hartwig.computeengine.storage.ImmutableGoogleStorageLocation;
import com.hartwig.gcp.StorageUtil;
import com.hartwig.pdl.ImmutablePipelineInput;
import com.hartwig.pdl.PipelineInput;
import com.hartwig.pdl.SampleInput;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.reference.api.PipelineOutputStructure;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

public class ReduxFileLocatorTest {
    private StorageUtil storageUtil;
    private String project;

    @Before
    public void setup() {
        storageUtil = mock(StorageUtil.class);
        project = Arguments.testDefaults().project();
    }

    @Test
    public void shouldDeriveFilesCorrectlyForPipeline5TumorBam() {
        var sampleName = TestInputs.tumorSample();
        var metadata = TestInputs.tumorRunMetadata();
        var pipelineInput =
                createTestPipelineInput(sampleName, String.format("gs://bucket/run/%s/aligner/%s.bam", sampleName, sampleName), true);

        var expectedJitterOutput = createExpectedOutput(String.format("run/%s/aligner/%s.jitter_params.tsv", sampleName, sampleName));
        var expectedMsTableOutput = createExpectedOutput(String.format("run/%s/aligner/%s.ms_table.tsv.gz", sampleName, sampleName));

        assertFilePathsDerivedCorrectly(expectedJitterOutput,
                expectedMsTableOutput,
                pipelineInput,
                metadata,
                PipelineOutputStructure.PIPELINE5);
    }

    @Test
    public void shouldDeriveFilesCorrectlyForPipeline5RefBam() {
        var sampleName = TestInputs.referenceSample();
        var metadata = TestInputs.referenceRunMetadata();
        var pipelineInput =
                createTestPipelineInput(sampleName, String.format("gs://bucket/run/%s/aligner/%s.bam", sampleName, sampleName), false);

        var expectedJitterOutput = createExpectedOutput(String.format("run/%s/aligner/%s.jitter_params.tsv", sampleName, sampleName));
        var expectedMsTableOutput = createExpectedOutput(String.format("run/%s/aligner/%s.ms_table.tsv.gz", sampleName, sampleName));

        assertFilePathsDerivedCorrectly(expectedJitterOutput,
                expectedMsTableOutput,
                pipelineInput,
                metadata,
                PipelineOutputStructure.PIPELINE5);
    }

    @Test
    public void shouldDeriveFilesCorrectlyForPipeline5TumorCram() {
        var sampleName = TestInputs.tumorSample();
        var metadata = TestInputs.tumorRunMetadata();
        var pipelineInput =
                createTestPipelineInput(sampleName, String.format("gs://bucket/run/%s/cram/%s.cram", sampleName, sampleName), true);

        var expectedJitterOutput = createExpectedOutput(String.format("run/%s/aligner/%s.jitter_params.tsv", sampleName, sampleName));
        var expectedMsTableOutput = createExpectedOutput(String.format("run/%s/aligner/%s.ms_table.tsv.gz", sampleName, sampleName));

        assertFilePathsDerivedCorrectly(expectedJitterOutput,
                expectedMsTableOutput,
                pipelineInput,
                metadata,
                PipelineOutputStructure.PIPELINE5);
    }

    @Test
    public void shouldDeriveFilesCorrectlyForPipeline5ReferenceCram() {
        var sampleName = TestInputs.referenceSample();
        var metadata = TestInputs.referenceRunMetadata();
        var pipelineInput =
                createTestPipelineInput(sampleName, String.format("gs://bucket/run/%s/cram/%s.cram", sampleName, sampleName), false);

        var expectedJitterOutput = createExpectedOutput(String.format("run/%s/aligner/%s.jitter_params.tsv", sampleName, sampleName));
        var expectedMsTableOutput = createExpectedOutput(String.format("run/%s/aligner/%s.ms_table.tsv.gz", sampleName, sampleName));

        assertFilePathsDerivedCorrectly(expectedJitterOutput,
                expectedMsTableOutput,
                pipelineInput,
                metadata,
                PipelineOutputStructure.PIPELINE5);
    }

    @Test
    public void shouldDeriveFilesCorrectlyForDatabaseTumorCram() {
        var sampleName = TestInputs.tumorSample();
        var metadata = TestInputs.tumorRunMetadata();
        var pipelineInput =
                createTestPipelineInput(sampleName, String.format("gs://bucket/%s/tumor/alignments/%s.cram", sampleName, sampleName), true);

        var expectedJitterOutput = createExpectedOutput(String.format("%s/tumor/alignments/%s.jitter_params.tsv", sampleName, sampleName));
        var expectedMsTableOutput = createExpectedOutput(String.format("%s/tumor/alignments/%s.ms_table.tsv.gz", sampleName, sampleName));

        assertFilePathsDerivedCorrectly(expectedJitterOutput,
                expectedMsTableOutput,
                pipelineInput,
                metadata,
                PipelineOutputStructure.DATABASE);
    }

    @Test
    public void shouldDeriveFilesCorrectlyForDatabaseReferenceCram() {
        var sampleName = TestInputs.referenceSample();
        var metadata = TestInputs.referenceRunMetadata();
        var pipelineInput = createTestPipelineInput(sampleName,
                String.format("gs://bucket/%s/reference/alignments/%s.cram", sampleName, sampleName),
                false);

        var expectedJitterOutput =
                createExpectedOutput(String.format("%s/reference/alignments/%s.jitter_params.tsv", sampleName, sampleName));
        var expectedMsTableOutput =
                createExpectedOutput(String.format("%s/reference/alignments/%s.ms_table.tsv.gz", sampleName, sampleName));

        assertFilePathsDerivedCorrectly(expectedJitterOutput,
                expectedMsTableOutput,
                pipelineInput,
                metadata,
                PipelineOutputStructure.DATABASE);
    }

    @Test
    public void shouldThrowErrorIfFileDoesNotExist() {
        var sampleName = TestInputs.referenceSample();
        var metadata = TestInputs.referenceRunMetadata();
        var pipelineInput = createTestPipelineInput(sampleName,
                String.format("gs://bucket/%s/reference/alignments/%s.cram", sampleName, sampleName),
                false);

        var victim = new ReduxFileLocator(pipelineInput, storageUtil, project, PipelineOutputStructure.PIPELINE5);

        assertThatThrownBy(() -> victim.locateJitterParamsFile(metadata)).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> victim.locateMsTableFile(metadata)).isInstanceOf(IllegalStateException.class);
    }

    private void assertFilePathsDerivedCorrectly(final ImmutableGoogleStorageLocation expectedJitterOutput,
            final ImmutableGoogleStorageLocation expectedMsTableOutput, final ImmutablePipelineInput pipelineInput,
            final SingleSampleRunMetadata metadata, PipelineOutputStructure outputStructure) {
        when(storageUtil.exists("gs://" + expectedJitterOutput.bucket() + "/" + expectedJitterOutput.path())).thenReturn(true);
        when(storageUtil.exists("gs://" + expectedMsTableOutput.bucket() + "/" + expectedMsTableOutput.path())).thenReturn(true);

        var victim = new ReduxFileLocator(pipelineInput, storageUtil, project, outputStructure);

        assertThat(victim.locateJitterParamsFile(metadata)).isEqualTo(expectedJitterOutput);
        assertThat(victim.locateMsTableFile(metadata)).isEqualTo(expectedMsTableOutput);
    }

    @NotNull
    private ImmutableGoogleStorageLocation createExpectedOutput(final String filePath) {
        return ImmutableGoogleStorageLocation.builder().bucket("bucket").path(filePath).billingProject(project).build();
    }

    @NotNull
    private static ImmutablePipelineInput createTestPipelineInput(final String sampleName, final String inputFile, final boolean isTumor) {
        if (isTumor) {
            return PipelineInput.builder()
                    .setName(TestInputs.SET)
                    .tumor(SampleInput.builder().name(sampleName).bam(inputFile).build())
                    .build();
        } else {
            return PipelineInput.builder()
                    .setName(TestInputs.SET)
                    .reference(SampleInput.builder().name(sampleName).bam(inputFile).build())
                    .build();
        }
    }
}
