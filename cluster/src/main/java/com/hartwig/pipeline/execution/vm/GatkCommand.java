package com.hartwig.pipeline.execution.vm;

import java.util.Arrays;
import java.util.List;

import com.google.common.collect.Lists;

public class GatkCommand extends JavaCommand {

    public GatkCommand(final String maxHeapSize, final String analysisType, final String... arguments) {
        super("/data/tools/GenomeAnalysisTK.jar", maxHeapSize, concat(Lists.newArrayList("-T", analysisType), arguments));
    }

    private static List<String> concat(List<String> first, String... rest) {
        first.addAll(Arrays.asList(rest));
        return first;
    }
}