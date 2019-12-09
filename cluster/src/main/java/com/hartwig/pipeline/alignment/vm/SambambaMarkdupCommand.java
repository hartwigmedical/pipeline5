package com.hartwig.pipeline.alignment.vm;

import com.google.common.collect.Lists;
import com.hartwig.pipeline.execution.vm.Bash;
import com.hartwig.pipeline.execution.vm.SambambaCommand;
import org.jetbrains.annotations.NotNull;

import java.util.List;

class SambambaMarkdupCommand extends SambambaCommand {

    SambambaMarkdupCommand(final List<String> inputBamPaths, final String outputBamPath) {
        super(arguments(inputBamPaths, outputBamPath));
    }

    @NotNull
    private static String[] arguments(final List<String> inputBamPaths, final String outputBamPath) {
        List<String> arguments = Lists.newArrayList("markdup", "-t", Bash.allCpus(), "--overflow-list-size=45000000");
        arguments.addAll(inputBamPaths);
        arguments.add(outputBamPath);
        return arguments.toArray(new String[arguments.size()]);
    }
}