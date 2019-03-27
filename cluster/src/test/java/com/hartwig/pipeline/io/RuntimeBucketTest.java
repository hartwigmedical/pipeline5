package com.hartwig.pipeline.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageClass;
import com.hartwig.pipeline.bootstrap.Arguments;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class RuntimeBucketTest {

    private static final String REGION = "region";
    private static final String SAMPLE_NAME = "test";
    private Storage storage;
    private ArgumentCaptor<BucketInfo> blobInfo;
    private Bucket bucket;

    @Before
    public void setUp() throws Exception {
        storage = mock(Storage.class);
        bucket = mock(Bucket.class);
        blobInfo = ArgumentCaptor.forClass(BucketInfo.class);
        when(storage.create(blobInfo.capture())).thenReturn(bucket);
    }

    @Test
    public void createsBucketIdFromSampleName() {
        RuntimeBucket.from(storage, SAMPLE_NAME, Arguments.testDefaultsBuilder().profile(Arguments.DefaultsProfile.PRODUCTION).build());
        assertThat(blobInfo.getValue().getName()).isEqualTo("run-test");
    }

    @Test
    public void setsRegionToArguments() {
        RuntimeBucket.from(storage, SAMPLE_NAME, Arguments.testDefaultsBuilder().region(REGION).build());
        assertThat(blobInfo.getValue().getLocation()).isEqualTo(REGION);
    }

    @Test
    public void usesRegionalStorageClass() {
        RuntimeBucket.from(storage, SAMPLE_NAME, Arguments.testDefaults());
        assertThat(blobInfo.getValue().getStorageClass()).isEqualTo(StorageClass.REGIONAL);
    }

    @Test
    public void cleanupDeletesAllObjectsBeforeDeletingBucket() {
        RuntimeBucket victim = RuntimeBucket.from(storage, SAMPLE_NAME, Arguments.testDefaults());
        @SuppressWarnings("unchecked")
        Page<Blob> page = mock(Page.class);
        Blob singleObject = mock(Blob.class);
        when(page.iterateAll()).thenReturn(Collections.singletonList(singleObject));
        when(bucket.exists()).thenReturn(true);
        when(bucket.list()).thenReturn(page);
        victim.cleanup();
        verify(singleObject, times(1)).delete();
        verify(bucket, times(1)).delete();
    }
}