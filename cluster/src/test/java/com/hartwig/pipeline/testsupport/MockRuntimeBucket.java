package com.hartwig.pipeline.testsupport;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.hartwig.pipeline.io.RuntimeBucket;

public class MockRuntimeBucket {

    private final RuntimeBucket runtimeBucket;
    private final Bucket googleBucket;
    private final List<Blob> blobs = new ArrayList<>();

    private MockRuntimeBucket(String name) {
        runtimeBucket = mock(RuntimeBucket.class);
        googleBucket = mock(Bucket.class);
        when(runtimeBucket.getName()).thenReturn(name);
        when(runtimeBucket.bucket()).thenReturn(googleBucket);
        Page page = mock(Page.class);
        //noinspection unchecked
        when(googleBucket.list(any())).thenReturn(page);
        when(page.iterateAll()).thenReturn(blobs);
    }

    public static MockRuntimeBucket of(String name) {
        return new MockRuntimeBucket(name);
    }

    public MockRuntimeBucket with(String blob, long size) {
        Blob mockBlob = mock(Blob.class);
        when(mockBlob.getName()).thenReturn(blob);
        when(mockBlob.getSize()).thenReturn(size);
        blobs.add(mockBlob);
        return this;
    }

    public RuntimeBucket getRuntimeBucket() {
        return runtimeBucket;
    }

    public Bucket getGoogleBucket() {
        return googleBucket;
    }
}
