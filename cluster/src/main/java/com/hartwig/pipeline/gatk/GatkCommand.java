package com.hartwig.pipeline.gatk;

import static com.hartwig.pipeline.tools.ExternalTool.GATK;

import java.util.Arrays;
import java.util.List;

import com.google.common.collect.Lists;

public class GatkCommand extends AdoptiumJava8JarCommand {

    public GatkCommand(final String maxHeapSize, final String analysisType, final String... arguments) {
        super(GATK.getToolName(),
                GATK.getVersion(),
                GATK.getBinary(),
                maxHeapSize,
                concat(Lists.newArrayList("-T", analysisType), arguments));
    }

    private static List<String> concat(final List<String> first, final String... rest) {
        first.addAll(Arrays.asList(rest));
        return first;
    }
}