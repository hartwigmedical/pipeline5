package com.hartwig.pipeline.io;

public class ResultsDirectory {

    private static final String PATH = "results/";
    private final String directory;

    private ResultsDirectory(final String directory) {
        this.directory = directory;
    }

    public String path(String subPath) {
        return directory + subPath;
    }

    public String path() {
        return directory;
    }

    public static ResultsDirectory defaultDirectory() {
        return new ResultsDirectory(PATH);
    }
}
