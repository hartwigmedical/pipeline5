package com.hartwig.pipeline.calling.somatic;

import java.util.ArrayList;

import com.google.common.collect.Lists;
import com.hartwig.pipeline.calling.command.BcfToolsCommand;

class BcfToolsPipeableExcludeFilterCommand extends BcfToolsCommand {
    BcfToolsPipeableExcludeFilterCommand(final String filter, final String type, final String inputVcf) {
        super(arguments(filter, type, inputVcf));
    }

    private static String[] arguments(final String filter, final String type, final String inputVcf) {
        ArrayList<String> arguments = Lists.newArrayList("filter", "-e", filter, "-s", type, "-m+", inputVcf, "-O", "u");
        return arguments.toArray(new String[arguments.size()]);
    }
}
