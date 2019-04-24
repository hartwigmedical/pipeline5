package com.hartwig.pipeline.calling.somatic;

import java.util.List;

import com.google.common.collect.Lists;

import org.jetbrains.annotations.NotNull;

class BcfToolsPipeableAnnotationCommand extends BcfToolsCommand {
    BcfToolsPipeableAnnotationCommand(String type) {
        super(arguments(type));
    }

    @NotNull
    private static String[] arguments(String type) {
        List<String> allArguments = Lists.newArrayList("annotate", "-x", type, "-O", "u");
        return allArguments.toArray(new String[allArguments.size()]);
    }
}
