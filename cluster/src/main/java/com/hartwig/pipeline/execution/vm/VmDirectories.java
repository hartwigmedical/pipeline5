package com.hartwig.pipeline.execution.vm;

public interface VmDirectories {
    String INPUT = "/data/input";
    String OUTPUT = "/data/output";
    String RESOURCES = "/data/resources";
    String TOOLS = "/opt/tools";

    static String outputFile(String path) {
        return String.format("%s/%s", OUTPUT, path);
    }
}
