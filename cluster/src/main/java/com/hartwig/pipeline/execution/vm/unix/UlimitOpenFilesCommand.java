package com.hartwig.pipeline.execution.vm.unix;

import com.hartwig.pipeline.execution.vm.BashCommand;

/**
 * Change the SOFT limit only for open files.
 */
public class UlimitOpenFilesCommand implements BashCommand {
    private int newSoftLimit;

    public UlimitOpenFilesCommand(int newSoftLimit) {
        this.newSoftLimit = newSoftLimit;
    }

    @Override
    public String asBash() {
        return "ulimit -Sn " + newSoftLimit;
    }
}
