package com.hartwig.pipeline.testsupport;

import com.google.api.gax.paging.Page;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.hartwig.pipeline.io.RuntimeBucket;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MockRuntimeBucket {

    private final RuntimeBucket runtimeBucket;
    private final Bucket googleBucket;
    private final List<Blob> blobs = new ArrayList<>();

    private MockRuntimeBucket(String name) {
        runtimeBucket = mock(RuntimeBucket.class);
        googleBucket = mock(Bucket.class);
        when(runtimeBucket.name()).thenReturn(name);
        when(runtimeBucket.bucket()).thenReturn(googleBucket);
        Page page = mock(Page.class);
        //noinspection unchecked
        when(googleBucket.list(any())).thenReturn(page);
        when(page.iterateAll()).thenReturn(blobs);
    }

    public static MockRuntimeBucket of(String name) {
        return new MockRuntimeBucket(name);
    }

    public static MockRuntimeBucket test() {
        return of("test");
    }

    public MockRuntimeBucket with(String blob, long size) {
        return with(blob, size, "");
    }

    public MockRuntimeBucket with(String blob, long size, String md5) {
        try {
            Blob mockBlob = mock(Blob.class);
            ReadChannel mockReadChannel = mock(ReadChannel.class);
            when(mockReadChannel.read(any())).thenReturn(-1);
            when(mockBlob.getName()).thenReturn(blob);
            when(mockBlob.getSize()).thenReturn(size);
            when(mockBlob.reader()).thenReturn(mockReadChannel);
            when(mockBlob.getMd5()).thenReturn(md5);
            blobs.add(mockBlob);
            when(googleBucket.get(blob)).thenReturn(mockBlob);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return this;
    }

    public RuntimeBucket getRuntimeBucket() {
        return runtimeBucket;
    }
}
