package com.hartwig.pipeline.calling.structural.gridss.command;

public interface GridssCommand {
    default int memoryGb() {
        return 8;
    }
    String className();
    String arguments();
}
