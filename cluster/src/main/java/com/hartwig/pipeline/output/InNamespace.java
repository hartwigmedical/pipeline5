package com.hartwig.pipeline.output;

import java.util.function.Predicate;

import com.google.cloud.storage.Blob;

public class InNamespace implements Predicate<Blob> {

    private final String namespace;

    private InNamespace(final String namespace) {
        this.namespace = namespace;
    }

    public static InNamespace of(final String namespace) {
        return new InNamespace(namespace);
    }

    @Override
    public boolean test(final Blob blob) {
        String[] path = blob.getName().split("/");
        if (path.length < 2) {
            return false;
        }
        String blobNamespace = path[path.length - 2];
        return namespace.equals(blobNamespace);
    }
}
