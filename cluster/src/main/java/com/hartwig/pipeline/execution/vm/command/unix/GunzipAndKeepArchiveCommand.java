package com.hartwig.pipeline.execution.vm.command.unix;

import com.hartwig.pipeline.execution.vm.BashCommand;

public class GunzipAndKeepArchiveCommand implements BashCommand {
    private final String archive;

    public GunzipAndKeepArchiveCommand(final String archive) {
        this.archive = archive;
    }

    @Override
    public String asBash() {
        return String.format("gunzip -kd %s", archive);
    }
}
