package com.hartwig.pipeline.io;

public class ResultsDirectory {

    private static final String DIRECTORY = "results";

    private ResultsDirectory() {
    }

    public String path(String subPath) {
        return path() + (subPath.startsWith("/") ? "" : "/") + subPath;
    }

    public String path() {
        return DIRECTORY;
    }

    public static ResultsDirectory defaultDirectory() {
        return new ResultsDirectory();
    }
}
