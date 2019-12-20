package com.hartwig.pipeline.tertiary;

import com.google.common.collect.Lists;
import com.hartwig.pipeline.execution.vm.Bash;
import com.hartwig.pipeline.execution.vm.JavaClassCommand;
import com.hartwig.pipeline.execution.vm.VmDirectories;

import java.util.Arrays;
import java.util.List;

public class HmfToolCommand extends JavaClassCommand {
    public HmfToolCommand(String toolName, String version, String jar, String mainClass, String maxHeap, String referenceSampleName,
            String referenceBamPath, String tumorSampleName, String tumorBamPath, String... arguments) {
        super(toolName,
                version,
                jar,
                mainClass,
                maxHeap,
                arguments(referenceSampleName, referenceBamPath, tumorSampleName, tumorBamPath, arguments));
    }

    private static String[] arguments(String referenceSampleName, String referenceBamPath, String tumorSampleName, String tumorBamPath,
            final String... arguments) {
        List<String> defaultArguments = Lists.newArrayList("-reference",
                referenceSampleName,
                "-reference_bam",
                referenceBamPath,
                "-tumor",
                tumorSampleName,
                "-tumor_bam",
                tumorBamPath,
                "-output_dir",
                VmDirectories.OUTPUT,
                "-threads",
                Bash.allCpus());
        defaultArguments.addAll(Arrays.asList(arguments));
        return defaultArguments.toArray(new String[]{});
    }
}
