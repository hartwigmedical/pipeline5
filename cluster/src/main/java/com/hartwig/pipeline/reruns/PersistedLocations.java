package com.hartwig.pipeline.reruns;

public class PersistedLocations {

    public static String blobForSingle(final String persistedSet, final String persistedSample, final String namespace, final String blobPath) {
        return String.format("%s/%s/%s/%s", persistedSet, persistedSample, namespace, blobPath);
    }

    public static String blobForSet(final String persistedSet, final String namespace, final String blobPath) {
        return String.format("%s/%s/%s", persistedSet, namespace, blobPath);
    }

    public static String pathForSet(final String persistedSet, final String namespace) {
        return String.format("%s/%s", persistedSet, namespace);
    }
}