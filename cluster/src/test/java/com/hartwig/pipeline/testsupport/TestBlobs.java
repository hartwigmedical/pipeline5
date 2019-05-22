package com.hartwig.pipeline.testsupport;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.storage.Blob;

public class TestBlobs {

    public static Blob blob(final String name) {
        Blob blob = mock(Blob.class);
        when(blob.getName()).thenReturn(name);
        return blob;
    }
}
