package com.hartwig.pipeline.rerun;

public class PersistedBlobLocation {

    static String of(final String persistedSet, final String persistedSample, final String namespace, final String blobPath) {
        return String.format("%s/%s/%s/%s", persistedSet, persistedSample, namespace, blobPath);
    }
}
