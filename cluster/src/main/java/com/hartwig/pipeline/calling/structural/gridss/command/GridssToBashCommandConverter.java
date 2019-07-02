package com.hartwig.pipeline.calling.structural.gridss.command;

import static java.lang.String.format;
import static java.util.Arrays.asList;

import java.util.List;

import com.hartwig.pipeline.execution.vm.JavaClassCommand;

public class GridssToBashCommandConverter {
    public JavaClassCommand convert(final GridssCommand gridssCommand) {
        List<String> jvmArgs = asList(
                "-Dsamjdk.create_index=true",
                "-Dsamjdk.use_async_io_read_samtools=true",
                "-Dsamjdk.use_async_io_write_samtools=true",
                "-Dsamjdk.use_async_io_write_tribble=true",
                "-Dsamjdk.buffer_size=4194304"
        );

        return new JavaClassCommand("gridss",
                "2.4.0",
                "gridss.jar",
                gridssCommand.className(),
                format("%dG", gridssCommand.memoryGb()),
                jvmArgs,
                gridssCommand.arguments());
    }
}
