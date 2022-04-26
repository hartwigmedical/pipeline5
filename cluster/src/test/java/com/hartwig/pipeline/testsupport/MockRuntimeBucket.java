package com.hartwig.pipeline.testsupport;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.hartwig.pipeline.storage.RuntimeBucket;

@SuppressWarnings("unchecked")
public class MockRuntimeBucket {

    private final RuntimeBucket runtimeBucket;
    private final List<Blob> blobs = new ArrayList<>();

    private MockRuntimeBucket(final String name) {
        runtimeBucket = mock(RuntimeBucket.class);
        when(runtimeBucket.name()).thenReturn(name);
        when(runtimeBucket.getNamespace()).thenReturn(name);
        when(runtimeBucket.runId()).thenReturn(name);
        when(runtimeBucket.list(any())).thenReturn(blobs);
        when(runtimeBucket.list()).thenReturn(blobs);
    }

    public static MockRuntimeBucket of(final String name) {
        return new MockRuntimeBucket(name);
    }

    public static MockRuntimeBucket test() {
        return of("test");
    }

    public MockRuntimeBucket with(final String blob, final long size) {
        try {
            Blob mockBlob = mock(Blob.class);
            ReadChannel mockReadChannel = mock(ReadChannel.class);
            when(mockReadChannel.read(any())).thenReturn(-1);
            when(mockBlob.getName()).thenReturn(blob);
            when(mockBlob.getSize()).thenReturn(size);
            when(mockBlob.reader()).thenReturn(mockReadChannel);
            when(mockBlob.getMd5()).thenReturn("");
            blobs.add(mockBlob);
            when(runtimeBucket.get(blob)).thenReturn(mockBlob);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return this;
    }

    public RuntimeBucket getRuntimeBucket() {
        return runtimeBucket;
    }
}
