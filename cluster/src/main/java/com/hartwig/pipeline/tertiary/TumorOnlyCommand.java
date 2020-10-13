package com.hartwig.pipeline.tertiary;

import java.util.Arrays;
import java.util.List;

import com.google.common.collect.Lists;
import com.hartwig.pipeline.execution.vm.Bash;
import com.hartwig.pipeline.execution.vm.JavaClassCommand;
import com.hartwig.pipeline.execution.vm.VmDirectories;

public class TumorOnlyCommand extends JavaClassCommand {
    public TumorOnlyCommand(String toolName, String version, String jar, String mainClass, String maxHeap, String tumorSampleName,
            String tumorBamPath, String... arguments) {
        super(toolName, version, jar, mainClass, maxHeap, arguments(tumorSampleName, tumorBamPath, arguments));
    }

    private static String[] arguments(String tumorSampleName, String tumorBamPath, final String... arguments) {
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
