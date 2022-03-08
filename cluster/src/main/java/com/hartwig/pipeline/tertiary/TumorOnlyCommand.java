package com.hartwig.pipeline.tertiary;

import java.util.Arrays;
import java.util.List;

import com.google.common.collect.Lists;
import com.hartwig.pipeline.execution.vm.Bash;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.execution.vm.java.JavaClassCommand;

public class TumorOnlyCommand extends JavaClassCommand {
    public TumorOnlyCommand(final String toolName, final String version, final String jar, final String mainClass, final String maxHeap, final String tumorSampleName,
            final String tumorBamPath, final String... arguments) {
        super(toolName, version, jar, mainClass, maxHeap, arguments(tumorSampleName, tumorBamPath, arguments));
    }

    private static String[] arguments(final String tumorSampleName, final String tumorBamPath, final String... arguments) {
        List<String> defaultArguments = Lists.newArrayList("-tumor_only",
                "-tumor",
                tumorSampleName,
                "-tumor_bam",
                tumorBamPath,
                "-output_dir",
                VmDirectories.OUTPUT,
                "-threads",
                Bash.allCpus());
        defaultArguments.addAll(Arrays.asList(arguments));
        return defaultArguments.toArray(new String[] {});
    }
}
