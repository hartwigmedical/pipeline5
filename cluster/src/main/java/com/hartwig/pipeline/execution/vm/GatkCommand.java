package com.hartwig.pipeline.execution.vm;

import com.google.common.collect.Lists;

import java.util.Arrays;
import java.util.List;

import static java.lang.String.format;

public class GatkCommand extends JavaCommand {

    public GatkCommand(final String maxHeapSize, final String analysisType, final String... arguments) {
        super(format("%s/gatk/3.8.0/GenomeAnalysisTK.jar", VmDirectories.TOOLS), maxHeapSize, concat(Lists.newArrayList("-T", analysisType), arguments));
    }

    private static List<String> concat(List<String> first, String... rest) {
        first.addAll(Arrays.asList(rest));
        return first;
    }
}