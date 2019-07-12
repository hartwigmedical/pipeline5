package com.hartwig.pipeline.calling.structural.gridss.command;

import static java.lang.String.format;
import static java.util.Arrays.asList;

import java.io.File;
import java.util.List;
import java.util.stream.Collectors;

import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.JavaClassCommand;

public abstract class GridssCommand implements BashCommand {
    int memoryGb() {
        return 8;
    }

    public abstract String className();

    public abstract List<GridssArgument> arguments();

    public static String basenameNoExtensions(final String completeFilename) {
        return new File(completeFilename).getName().split("\\.")[0];
    }

    @Override
    public String asBash() {
        List<String> jvmArgs = asList("-Dsamjdk.create_index=true",
                "-Dsamjdk.use_async_io_read_samtools=true",
                "-Dsamjdk.use_async_io_write_samtools=true",
                "-Dsamjdk.use_async_io_write_tribble=true",
                "-Dsamjdk.buffer_size=4194304");

        String gridssArgs = arguments().stream().map(GridssArgument::asBash).collect(Collectors.joining(" "));
        return new JavaClassCommand("gridss", "2.4.0", "gridss.jar", className(), format("%dG", memoryGb()), jvmArgs, gridssArgs).asBash();
    }
}
