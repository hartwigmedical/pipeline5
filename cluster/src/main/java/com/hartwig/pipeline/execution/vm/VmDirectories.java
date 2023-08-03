package com.hartwig.pipeline.execution.vm;

public interface VmDirectories {
    String INPUT = "/data/input";
    String OUTPUT = "/data/output";
    String RESOURCES = "/opt/resources";
    String TOOLS = "/opt/tools";
    String TEMP = "/data/tmp";

    static String outputFile(final String path) {
        return filePath(OUTPUT, path);
    }

    static String resourcesPath(final String path) {
        return filePath(RESOURCES, path);
    }

    static String toolPath(final String path) {
        return filePath(TOOLS, path);
    }

    static String filePath(final String directory, final String path) {
        return String.format("%s/%s", directory, path);
    }

}
