package com.hartwig.pipeline.execution.vm;

import static java.lang.String.format;

public class CopyResourceToOutput implements BashCommand {
    private final String localResourcePath;

    public CopyResourceToOutput(final String localResourcePath) {
        this.localResourcePath = localResourcePath;
    }

    @Override
    public String asBash() {
        return format("cp %s %s", localResourcePath, VmDirectories.OUTPUT);
    }
}
