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

    public static void addToStorage(final Storage storage, final String resourceName, final String... fileNames) {
        Bucket referenceGenomeBucket = mock(Bucket.class);
        List<Blob> blobs = new ArrayList<>();
        for (String fileName : fileNames) {
            Blob blob = mock(Blob.class);
            when(blob.getName()).thenReturn(fileName);
            blobs.add(blob);
        }
        @SuppressWarnings("unchecked")
        Page<Blob> page = mock(Page.class);
        when(page.iterateAll()).thenReturn(blobs);
        when(storage.get(resourceName)).thenReturn(referenceGenomeBucket);
        when(referenceGenomeBucket.list()).thenReturn(page);
    }
}
