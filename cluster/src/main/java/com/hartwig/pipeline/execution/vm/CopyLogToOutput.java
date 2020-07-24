package com.hartwig.pipeline.execution.vm;

import static java.lang.String.format;

public class CopyLogToOutput implements BashCommand {

    private final String command;

    public CopyLogToOutput(final RuntimeFiles runtimeFiles) {
        this(runtimeFiles.log(), runtimeFiles.log());
    }

    public CopyLogToOutput(final String inputName, final String outputName) {
        command = inputName.equals(outputName)
                ? format("cp %s/%s %s", BashStartupScript.LOCAL_LOG_DIR, inputName, VmDirectories.OUTPUT)
                : format("cp %s/%s %s/%s", BashStartupScript.LOCAL_LOG_DIR, inputName, VmDirectories.OUTPUT, outputName);
    }

    @Override
    public String asBash() {
        return command;
    }
}
