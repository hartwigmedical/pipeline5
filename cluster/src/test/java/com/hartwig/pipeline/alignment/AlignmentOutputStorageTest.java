package com.hartwig.pipeline.alignment;

import static com.hartwig.pipeline.io.GoogleStorageLocation.of;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.io.GoogleStorageLocation;
import com.hartwig.pipeline.io.ResultsDirectory;
import com.hartwig.pipeline.testsupport.TestSamples;

import org.junit.Before;
import org.junit.Test;

public class AlignmentOutputStorageTest {

    private static final ResultsDirectory RESULTS_DIRECTORY = ResultsDirectory.defaultDirectory();
    private static final String BUCKET_NAME = "run-sample";
    private static final String NAMESPACED_BUCKET_NAME = BUCKET_NAME + "/aligner";
    private static final String SORTED_PATH = RESULTS_DIRECTORY.path("sample.sorted.bam");
    private static final String SORTED_BAI_PATH = RESULTS_DIRECTORY.path("sample.sorted.bam.bai");
     private static final AlignmentOutput EXPECTED_OUTPUT = AlignmentOutput.builder()
            .status(PipelineStatus.SUCCESS)
            .maybeFinalBamLocation(GoogleStorageLocation.of(NAMESPACED_BUCKET_NAME, SORTED_PATH))
            .maybeFinalBaiLocation(of(NAMESPACED_BUCKET_NAME, SORTED_BAI_PATH))
            .sample(TestSamples.simpleReferenceSample())
            .build();
    private AlignmentOutputStorage victim;
    private Bucket outputBucket;

    @Before
    public void setUp() throws Exception {
        final Storage storage = mock(Storage.class);
        victim = new AlignmentOutputStorage(storage, Arguments.testDefaults(), RESULTS_DIRECTORY);
        outputBucket = mock(Bucket.class);
        when(outputBucket.getName()).thenReturn(BUCKET_NAME);
        when(storage.get(BUCKET_NAME)).thenReturn(outputBucket);
    }

    @Test
    public void returnsDataLocationsForExistingSample() {
        mockBlob(SORTED_PATH);
        mockBlob(SORTED_BAI_PATH);
        assertThat(victim.get(TestSamples.simpleReferenceSample())).hasValue(EXPECTED_OUTPUT);
    }

    @Test
    public void returnsEmptyIfNoMetricsInBucket() {
        mockBlob(SORTED_PATH);
        assertThat(victim.get(TestSamples.simpleReferenceSample())).isEmpty();
    }

    private void mockBlob(final String blobPath) {
        Blob blob = blob(blobPath);
        when(outputBucket.get(Aligner.NAMESPACE + "/" + blobPath)).thenReturn(blob);
    }

    private Blob blob(String name) {
        Blob blob = mock(Blob.class);
        when(blob.getName()).thenReturn(Aligner.NAMESPACE + "/" + name);
        return blob;
    }
}