package com.hartwig.pipeline.calling.structural.gridss.command;

import com.hartwig.pipeline.execution.vm.JavaClassCommand;

import java.util.List;

import static java.lang.String.format;
import static java.util.Arrays.asList;

public class GridssToBashCommandConverter {
    public JavaClassCommand convert(GridssCommand gridssCommand) {
        List<String> jvmArgs = asList(
                "-ea",
                "-Dsamjdk.create_index=true",
                "-Dsamjdk.use_async_io_read_samtools=true",
                "-Dsamjdk.use_async_io_write_samtools=true",
                "-Dsamjdk.use_async_io_write_tribble=true",
                "-Dsamjdk.buffer_size=4194304"
        );

        return new JavaClassCommand("gridss",
                "2.2.2",
                "gridss.jar",
                gridssCommand.className(),
                format("%dG", gridssCommand.memoryGb()),
                jvmArgs,
                gridssCommand.arguments());
    }


}
