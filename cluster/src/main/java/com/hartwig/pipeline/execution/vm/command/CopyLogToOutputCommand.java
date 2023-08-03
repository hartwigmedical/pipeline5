package com.hartwig.pipeline.execution.vm.command;

import static java.lang.String.format;

import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.RuntimeFiles;
import com.hartwig.pipeline.execution.vm.VmDirectories;

public class CopyLogToOutputCommand implements BashCommand {

    private final String command;

    public CopyLogToOutputCommand(final RuntimeFiles runtimeFiles) {
        this(runtimeFiles.log(), runtimeFiles.log());
    }

    public CopyLogToOutputCommand(final String inputName, final String outputName) {
        command = inputName.equals(outputName)
                ? String.format("cp %s/%s %s", BashStartupScript.LOCAL_LOG_DIR, inputName, VmDirectories.OUTPUT)
                : format("cp %s/%s %s/%s", BashStartupScript.LOCAL_LOG_DIR, inputName, VmDirectories.OUTPUT, outputName);
    }

    @Override
    public String asBash() {
        return command;
    }
}
