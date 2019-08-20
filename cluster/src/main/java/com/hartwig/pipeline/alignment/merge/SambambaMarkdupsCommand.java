package com.hartwig.pipeline.alignment.merge;

import java.util.List;

import com.google.common.collect.Lists;
import com.hartwig.pipeline.calling.command.VersionedToolCommand;
import com.hartwig.pipeline.execution.vm.Bash;
import com.hartwig.pipeline.tools.Versions;

import org.jetbrains.annotations.NotNull;

class SambambaMarkdupsCommand extends VersionedToolCommand {

    SambambaMarkdupsCommand(final List<String> inputBamPaths, final String outputBamPath) {
        super("sambamba", "sambamba", Versions.SAMBAMBA, arguments(inputBamPaths, outputBamPath));
    }

    @NotNull
    private static String[] arguments(final List<String> inputBamPaths, final String outputBamPath) {
        List<String> arguments = Lists.newArrayList("markdup", "-t", Bash.allCpus(), "--overflow-list-size=15000000");
        arguments.addAll(inputBamPaths);
        arguments.add(outputBamPath);
        return arguments.toArray(new String[arguments.size()]);
    }
}
