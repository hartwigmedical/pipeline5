package com.hartwig.pipeline.calling.structural.gridss.command;

import static com.hartwig.pipeline.tools.HmfTool.GRIDSS;

import java.util.List;

import com.google.api.client.util.Lists;
import com.google.common.collect.ImmutableList;
import com.hartwig.computeengine.execution.vm.command.java.JavaClassCommand;

public class GridssCommand extends JavaClassCommand {
    public static final List<String> JVM_ARGUMENTS = ImmutableList.of("-Dsamjdk.create_index=true",
            "-Dsamjdk.use_async_io_read_samtools=true",
            "-Dsamjdk.use_async_io_write_samtools=true",
            "-Dsamjdk.use_async_io_write_tribble=true",
            "-Dsamjdk.buffer_size=4194304");

    GridssCommand(final String className, final String maxHeap, final List<String> jvmArguments, final String... arguments) {
        super(GRIDSS.getToolName(), GRIDSS.runVersion(), GRIDSS.jar(), className, maxHeap, mergeJvmArguments(jvmArguments), arguments);
    }

    private static List<String> mergeJvmArguments(final List<String> jvmArguments) {
        List<String> results = Lists.newArrayList(JVM_ARGUMENTS);
        results.addAll(jvmArguments);
        return results;
    }
}
