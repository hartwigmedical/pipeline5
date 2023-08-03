package com.hartwig.pipeline.execution.vm.command.unix;

import com.hartwig.pipeline.execution.vm.command.BashCommand;

public class UnzipToDirectoryCommand implements BashCommand {
    private final String outDir;
    private final String zipFile;

    public UnzipToDirectoryCommand(final String outDir, final String zipFile) {
        this.outDir = outDir;
        this.zipFile = zipFile;
    }

    @Override
    public String asBash() {
        return String.format("unzip -d %s %s", outDir, zipFile);
    }
}
