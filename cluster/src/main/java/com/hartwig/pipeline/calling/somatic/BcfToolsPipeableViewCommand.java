package com.hartwig.pipeline.calling.somatic;

import java.util.List;

import com.google.common.collect.Lists;
import com.hartwig.pipeline.calling.command.BcfToolsCommand;

import org.jetbrains.annotations.NotNull;

class BcfToolsPipeableViewCommand extends BcfToolsCommand {
    BcfToolsPipeableViewCommand(final String tumorSampleName, final String outputVcf) {
        super(arguments(tumorSampleName, outputVcf));
    }

    @NotNull
    private static String[] arguments(final String tumorSampleName, final String outputVcf) {
        List<String> allArguments = Lists.newArrayList("view", "-s", tumorSampleName, "-O", "z", "-o", outputVcf);
        return allArguments.toArray(new String[allArguments.size()]);
    }
}
