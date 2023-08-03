package com.hartwig.pipeline.execution.vm.command;

import java.util.List;

public class DeleteFilesCommand implements BashCommand {
    private final List<String> files;

    public DeleteFilesCommand(final List<String> files) {
        this.files = files;
    }

    @Override
    public String asBash() {
        return String.format("rm %s", String.join(" ", files));
    }
}
