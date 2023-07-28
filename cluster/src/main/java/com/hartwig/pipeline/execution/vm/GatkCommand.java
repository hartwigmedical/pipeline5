package com.hartwig.pipeline.execution.vm;

import static com.hartwig.pipeline.tools.ExternalTool.GATK;

import java.util.Arrays;
import java.util.List;

import com.google.common.collect.Lists;
import com.hartwig.pipeline.execution.vm.java.Java8JarCommand;

public class GatkCommand extends Java8JarCommand {

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