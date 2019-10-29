package com.hartwig.pipeline.cleanup;

import static com.hartwig.pipeline.testsupport.TestBlobs.blob;
import static com.hartwig.pipeline.testsupport.TestBlobs.pageOf;
import static com.hartwig.pipeline.testsupport.TestInputs.defaultSomaticRunMetadata;

import static org.mockito.ArgumentMatchers.any;
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
import com.hartwig.pipeline.ImmutableArguments;
import com.hartwig.pipeline.testsupport.TestBlobs;

import org.junit.Before;
import org.junit.Test;

public class CleanupTest {

    private static final ImmutableArguments ARGUMENTS = Arguments.testDefaultsBuilder().cleanup(true).build();
    private static final String RUN_REFERENCE = "run-reference-test";
    private static final String RUN_TUMOR = "run-tumor-test";
    private Storage storage;
    private Bucket referenceBucket;
    private Cleanup victim;
    private Bucket tumorBucket;
    private Bucket somaticBucket;
    private Bucket stagingBucket;

    @Before
    public void setUp() throws Exception {
        storage = mock(Storage.class);
        referenceBucket = mock(Bucket.class);
        tumorBucket = mock(Bucket.class);
        somaticBucket = mock(Bucket.class);

        stagingBucket = mock(Bucket.class);
        when(storage.get(ARGUMENTS.patientReportBucket())).thenReturn(stagingBucket);
        Page<Blob> page = TestBlobs.pageOf();
        when(stagingBucket.list(any())).thenReturn(page);

        victim = new Cleanup(storage, ARGUMENTS);
    }

    @Test
    public void doesNothingWhenCleanupDisabled() {
        victim = new Cleanup(storage, Arguments.testDefaultsBuilder().cleanup(false).build());
        victim.run(defaultSomaticRunMetadata());
        verify(referenceBucket, never()).delete();
    }

    @Test
    public void deletesReferenceBucketIfExists() {
        assertBucketDeleted(RUN_REFERENCE, referenceBucket);
    }

    @Test
    public void deletesTumorBucketIfExists() {
        assertBucketDeleted(RUN_TUMOR, tumorBucket);
    }

    @Test
    public void deletesSomaticBucketIfExists() {
        assertBucketDeleted("run-reference-tumor-test", somaticBucket);
    }

    @Test
    public void deletesStagingBucketIfExists() {
        Blob output = TestBlobs.blob("output.txt");
        Page<Blob> page = TestBlobs.pageOf(output);
        when(stagingBucket.list(Storage.BlobListOption.prefix("reference-test"))).thenReturn(page);
        victim.run(defaultSomaticRunMetadata());
        verify(output, times(1)).delete();
    }

    private void assertBucketDeleted(final String bucketName, final Bucket bucket) {
        Blob blob = returnBlob(bucketName, bucket);
        victim.run(defaultSomaticRunMetadata());
        verify(bucket, times(1)).delete();
        verify(blob, times(1)).delete();
    }

    private Blob returnBlob(final String bucketName, final Bucket bucket) {
        when(storage.get(bucketName)).thenReturn(bucket);
        Blob blob = blob("result");
        Page<Blob> page = pageOf(blob);
        when(bucket.list()).thenReturn(page);
        return blob;
    }
}