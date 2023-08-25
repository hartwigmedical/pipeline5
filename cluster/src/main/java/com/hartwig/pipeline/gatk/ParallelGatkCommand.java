package com.hartwig.pipeline.gatk;

import com.google.common.collect.Lists;
import com.hartwig.computeengine.execution.vm.Bash;

import java.util.Arrays;
import java.util.List;

public class ParallelGatkCommand extends GatkCommand {

    public ParallelGatkCommand(final String maxHeapSize, final String analysisType, final String... arguments) {
        super(maxHeapSize, analysisType, concat(Lists.newArrayList("-nct", Bash.allCpus()), arguments));
    }

    private static String[] concat(final List<String> first, final String... rest) {
        first.addAll(Arrays.asList(rest));
        return first.toArray(new String[first.size()]);
    }
}