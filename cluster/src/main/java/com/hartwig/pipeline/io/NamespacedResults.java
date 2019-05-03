package com.hartwig.pipeline.io;

public class NamespacedResults {

    private static final String DIRECTORY = "results";
    private final String namespace;

    private NamespacedResults(final String namespace) {
        this.namespace = namespace;
    }

    public String path(String subPath) {
        return path() + (subPath.startsWith("/") ? "" : "/") + subPath;
    }

    public String path() {
        return String.format("%s/%s", DIRECTORY, namespace);
    }

    public static NamespacedResults of(String namespace) {
        return new NamespacedResults(namespace);
    }
}
