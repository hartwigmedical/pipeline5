package com.hartwig.pipeline.testsupport;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;

public class TestBlobs {

    public static Blob blob(final String name) {
        Blob blob = mock(Blob.class);
        when(blob.getName()).thenReturn(name);
        return blob;
    }

    @SuppressWarnings("unchecked")
    public static Page<Blob> pageOf(final Blob... blobs) {
        Page<Blob> page = mock(Page.class);
        when(page.iterateAll()).thenReturn(Arrays.asList(blobs));
        return page;
    }
}