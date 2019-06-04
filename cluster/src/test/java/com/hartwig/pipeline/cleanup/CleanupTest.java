package com.hartwig.pipeline.cleanup;

import static com.hartwig.pipeline.testsupport.TestBlobs.blob;
import static com.hartwig.pipeline.testsupport.TestBlobs.pageOf;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Before;
import org.junit.Test;

public class CleanupTest {

    private Storage storage;
    private Bucket referenceBucket;
    private Cleanup victim;
    private Bucket tumorBucket;
    private Bucket somaticBucket;

    @Before
    public void setUp() throws Exception {
        storage = mock(Storage.class);
        referenceBucket = mock(Bucket.class);
        tumorBucket = mock(Bucket.class);
        somaticBucket = mock(Bucket.class);
        victim = new Cleanup(storage, Arguments.testDefaultsBuilder().cleanup(true).build());
    }

    @Test
    public void doesNothingWhenCleanupDisabled() {
        victim = new Cleanup(storage, Arguments.testDefaultsBuilder().cleanup(false).build());
        victim.run(TestInputs.defaultPair());
        verify(referenceBucket, never()).delete();
    }

    @Test
    public void deletesReferenceBucketIfExists() {
        assertBucketDeleted("run-reference", referenceBucket);
    }

    @Test
    public void deletesTumorBucketIfExists() {
        assertBucketDeleted("run-tumor", tumorBucket);
    }

    @Test
    public void deletesSomaticBucketIfExists() {
        assertBucketDeleted("run-reference-tumor", somaticBucket);
    }

    private void assertBucketDeleted(final String bucketName, final Bucket bucket) {
        when(storage.get(bucketName)).thenReturn(bucket);
        Blob blob = blob("result");
        Page<Blob> page = pageOf(blob);
        when(bucket.list()).thenReturn(page);
        victim.run(TestInputs.defaultPair());
        verify(bucket, times(1)).delete();
        verify(blob, times(1)).delete();
    }
}