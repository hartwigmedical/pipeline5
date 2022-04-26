package com.hartwig.pipeline.execution.vm.unix;

import static java.lang.String.format;

import java.io.File;

import com.hartwig.pipeline.calling.command.VersionedToolCommand;

public class ExportPathCommand extends ExportVariableCommand {

    public ExportPathCommand(final String path) {
        super("PATH", format("${PATH}:%s", path));
    }

    public ExportPathCommand(final VersionedToolCommand command) {
        this(dirname(command.asBash()));
    }

    private static String dirname(final String filename) {
        return new File(filename).getParent();
    }
}
