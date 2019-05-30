package com.hartwig.pipeline.execution.vm;

import com.google.common.collect.Lists;
import com.hartwig.pipeline.tools.Versions;

import java.util.Arrays;
import java.util.List;

import static java.lang.String.format;

public class GatkCommand extends JavaJarCommand {

    public GatkCommand(final String maxHeapSize, final String analysisType, final String ... arguments) {
        super("gatk", Versions.GATK, "GenomeAnalysisTK.jar", maxHeapSize, concat(Lists.newArrayList("-T", analysisType), arguments));
    }

    private static List<String> concat(List<String> first, String... rest) {
        first.addAll(Arrays.asList(rest));
        return first;
    }
}