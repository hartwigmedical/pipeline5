package com.hartwig.pipeline.execution.vm;

public interface VmDirectories {
    String INPUT = "/data/input";
    String OUTPUT = "/data/output";
    String RESOURCES = "/opt/resources";
    String TOOLS = "/opt/tools";
    String TEMP = "/data/tmp";

    static String outputFile(String path) {
        return String.format("%s/%s", OUTPUT, path);
    }
}
