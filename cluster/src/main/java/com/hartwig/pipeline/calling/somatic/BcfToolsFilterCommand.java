package com.hartwig.pipeline.calling.somatic;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Lists;

import org.jetbrains.annotations.NotNull;

class BcfToolsFilterCommand extends BcfToolsCommand {
    BcfToolsFilterCommand(final String filter, final String type, final String inputVcf, final String outputVcf) {
        super(arguments(filter, type, inputVcf, outputVcf));
    }

    @NotNull
    private static String[] arguments(final String filter, final String type, final String inputVcf, final String outputVcf) {
        ArrayList<String> arguments = Lists.newArrayList("filter", "-e", filter, "-s", type, "-m+", inputVcf, "-O", "z", "-o", outputVcf);
        return arguments.toArray(new String[arguments.size()]);
    }
}
