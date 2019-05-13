package com.hartwig.pipeline.testsupport;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;

public class MockResource {

    private static Bucket RESOURCE_BUCKET;

    public static void addToStorage(final Storage storage, final String resourceName, final String... fileNames) {
        Bucket resourceBucket = getResourceBucket(storage);
        List<Blob> blobs = new ArrayList<>();
        for (String fileName : fileNames) {
            Blob blob = mock(Blob.class);
            when(blob.getName()).thenReturn(fileName);
            blobs.add(blob);
        }
        @SuppressWarnings("unchecked")
        Page<Blob> page = mock(Page.class);
        when(page.iterateAll()).thenReturn(blobs);
        when(storage.get("common-resources")).thenReturn(resourceBucket);
        when(resourceBucket.list(Storage.BlobListOption.prefix(resourceName))).thenReturn(page);
    }

    private static Bucket getResourceBucket(final Storage storage) {
        if (RESOURCE_BUCKET == null){
            RESOURCE_BUCKET = mock(Bucket.class);
            when(storage.get("common-resources")).thenReturn(RESOURCE_BUCKET);
        }
        return RESOURCE_BUCKET;
    }
}
