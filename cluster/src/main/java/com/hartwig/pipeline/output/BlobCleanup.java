package com.hartwig.pipeline.output;

import java.util.function.Consumer;

import com.google.cloud.storage.Blob;

public class BlobCleanup implements Consumer<Blob> {
    @Override
    public void accept(final Blob blob) {
        blob.delete();
    }
}
