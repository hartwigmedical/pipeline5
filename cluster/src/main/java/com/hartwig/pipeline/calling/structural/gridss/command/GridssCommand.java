package com.hartwig.pipeline.calling.structural.gridss.command;

import java.util.List;

import com.google.api.client.util.Lists;
import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.execution.vm.JavaClassCommand;
import com.hartwig.pipeline.tools.Versions;

class GridssCommand extends JavaClassCommand {
    private static final List<String> JVM_ARGUMENTS = ImmutableList.of("-Dsamjdk.create_index=true",
            "-Dsamjdk.use_async_io_read_samtools=true",
            "-Dsamjdk.use_async_io_write_samtools=true",
            "-Dsamjdk.use_async_io_write_tribble=true",
            "-Dsamjdk.buffer_size=4194304");

    GridssCommand(final String className, final String maxHeap, final List<String> jvmArguments, final String... arguments) {
        super("gridss", Versions.GRIDSS, "gridss.jar", className, maxHeap, mergeJvmArguments(jvmArguments), arguments);
    }

    private static List<String> mergeJvmArguments(final List<String> jvmArguments) {
        List<String> results = Lists.newArrayList(JVM_ARGUMENTS);
        results.addAll(jvmArguments);
        return results;
    }

}
