package com.hartwig.pipeline.calling.somatic;

import java.util.ArrayList;

import com.google.common.collect.Lists;

import org.jetbrains.annotations.NotNull;

class BcfToolsPipeableIncludeFilterCommand extends BcfToolsCommand {
    BcfToolsPipeableIncludeFilterCommand(final String filter, final String inputVcf) {
        super(arguments(filter, inputVcf));
    }

    @NotNull
    private static String[] arguments(final String filter, final String inputVcf) {
        ArrayList<String> arguments = Lists.newArrayList("filter", "-i", filter, inputVcf, "-O", "u");
        return arguments.toArray(new String[arguments.size()]);
    }
}
