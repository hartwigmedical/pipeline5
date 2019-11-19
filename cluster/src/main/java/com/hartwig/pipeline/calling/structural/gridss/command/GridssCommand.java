package com.hartwig.pipeline.calling.structural.gridss.command;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.execution.vm.JavaClassCommand;
import com.hartwig.pipeline.tools.Versions;

class GridssCommand extends JavaClassCommand {

    GridssCommand(final String className, final String maxHeap, final String... arguments) {
        super("gridss",
                Versions.GRIDSS,
                "gridss.jar",
                className,
                maxHeap,
                ImmutableList.of("-Dsamjdk.create_index=true",
                        "-Dsamjdk.use_async_io_read_samtools=true",
                        "-Dsamjdk.use_async_io_write_samtools=true",
                        "-Dsamjdk.use_async_io_write_tribble=true",
                        "-Dsamjdk.buffer_size=4194304"),
                arguments);
    }
}
