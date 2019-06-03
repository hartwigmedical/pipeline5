package com.hartwig.pipeline.calling.command;

import java.util.List;

import com.google.common.collect.Lists;

import org.jetbrains.annotations.NotNull;

public class BcfToolsAnnotationCommand extends BcfToolsCommand {
    public BcfToolsAnnotationCommand(List<String> annotationArguments, final String inputVcf, final String outputVcf) {
        super(arguments(annotationArguments, inputVcf, outputVcf));
    }

    @NotNull
    private static String[] arguments(List<String> annotationArguments, final String inputVcf, final String outputVcf) {
        List<String> allArguments = Lists.newArrayList("annotate", "-a");
        allArguments.addAll(annotationArguments);
        allArguments.addAll(Lists.newArrayList("-o", outputVcf, "-O", "z", inputVcf));
        return allArguments.toArray(new String[allArguments.size()]);
    }
}
