package com.hartwig.pipeline;

import java.util.Arrays;
import java.util.List;

import com.google.common.collect.Lists;
import com.hartwig.pipeline.execution.vm.Bash;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.execution.vm.java.JavaClassCommand;

public class GermlineOnlyCommand extends JavaClassCommand {
    public GermlineOnlyCommand(final String toolName, final String version, final String jar, final String mainClass, final String maxHeap,
            final String tumorSampleName, final String tumorBamPath, final String... arguments) {
        super(toolName, version, jar, mainClass, maxHeap, arguments(tumorSampleName, tumorBamPath, arguments));
    }

    private static String[] arguments(final String referenceSampleName, final String referenceBamPath, final String... arguments) {
        List<String> defaultArguments = Lists.newArrayList("-reference",
                referenceSampleName,
                "-reference_bam",
                referenceBamPath,
                "-output_dir",
                VmDirectories.OUTPUT,
                "-threads",
                Bash.allCpus());
        defaultArguments.addAll(Arrays.asList(arguments));
        return defaultArguments.toArray(new String[] {});
    }
}
