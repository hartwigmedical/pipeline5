package com.hartwig.pipeline.alignment;

import static com.hartwig.pipeline.io.GoogleStorageLocation.of;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.io.ResultsDirectory;
import com.hartwig.pipeline.testsupport.TestSamples;

import org.junit.Before;
import org.junit.Test;

public class AlignmentOutputStorageTest {

    private static final String BUCKET_NAME = "run-sample";
    private static final String SORTED_PATH = "results/sample.sorted.bam";
    private static final String RECALIBRATED_PATH = "results/sample.recalibrated.bam";
    private static final AlignmentOutput EXPECTED_OUTPUT =
            AlignmentOutput.of(of(BUCKET_NAME, SORTED_PATH), of(BUCKET_NAME, RECALIBRATED_PATH), TestSamples.simpleReferenceSample());
    private Storage storage;
    private AlignmentOutputStorage victim;
    private Bucket outputBucket;

    @Before
    public void setUp() throws Exception {
        storage = mock(Storage.class);
        victim = new AlignmentOutputStorage(storage, Arguments.testDefaults(), ResultsDirectory.defaultDirectory());
        outputBucket = mock(Bucket.class);
        when(outputBucket.getName()).thenReturn(BUCKET_NAME);
        when(storage.get(BUCKET_NAME)).thenReturn(outputBucket);
    }

    @Test
    public void returnsEmptyOptionalWhenNoOutputBucketFound() {
        when(storage.get(BUCKET_NAME)).thenReturn(null);
        Sample sample = TestSamples.simpleReferenceSample();
        assertThat(victim.get(sample)).isEmpty();
    }

    @Test
    public void returnsDatalocationsForExistingSample() {
        mockBlob(SORTED_PATH);
        mockBlob(RECALIBRATED_PATH);
        assertThat(victim.get(TestSamples.simpleReferenceSample())).hasValue(EXPECTED_OUTPUT);
    }

    @Test
    public void returnsEmptyIfNoMetricsInBucket() {
        mockBlob(SORTED_PATH);
        assertThat(victim.get(TestSamples.simpleReferenceSample())).isEmpty();
    }

    @Test(expected = IllegalStateException.class)
    public void throwsIllegalStateWhenMetricsExistButNoBams() {
        mockBlob(RECALIBRATED_PATH);
        victim.get(TestSamples.simpleReferenceSample());
    }

    private void mockBlob(final String blobPath) {
        Blob blob = blob(blobPath);
        when(outputBucket.get(blobPath)).thenReturn(blob);
    }

    private Blob blob(String name) {
        Blob blob = mock(Blob.class);
        when(blob.getName()).thenReturn(name);
        return blob;
    }
}