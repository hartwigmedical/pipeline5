package com.hartwig.pipeline.tertiary.amber;

import java.util.Arrays;
import java.util.List;

import com.google.common.collect.Lists;
import com.hartwig.pipeline.execution.vm.Bash;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.execution.vm.java.JavaClassCommand;
import com.hartwig.pipeline.execution.vm.java.JavaJarCommand;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.tertiary.TumorNormalCommand;
import com.hartwig.pipeline.tertiary.TumorOnlyCommand;
import com.hartwig.pipeline.tools.Versions;

public class AmberApplicationCommand implements BashCommand {

    private static final String JAR = "amber.jar";
    private static final String MAX_HEAP = "32G";
    private static final String MAIN_CLASS = "com.hartwig.hmftools.amber.AmberApplication";

    private final String bash;

    public AmberApplicationCommand(ResourceFiles resourceFiles, String tumorSampleName, String tumorBamPath) {
        bash = new TumorOnlyCommand("amber",
                Versions.AMBER,
                JAR,
                MAIN_CLASS,
                MAX_HEAP,
                tumorSampleName,
                tumorBamPath,
                "-ref_genome",
                resourceFiles.refGenomeFile(),
                "-loci",
                resourceFiles.amberHeterozygousLoci()).asBash();
    }

    public AmberApplicationCommand(ResourceFiles resourceFiles, String referenceSampleName, String referenceBamPath, String tumorSampleName,
            String tumorBamPath) {
        bash = new JavaClassCommand("amber",
                Versions.AMBER,
                JAR,
                MAIN_CLASS,
                MAX_HEAP,
                Lists.newArrayList("-tumor_only",
                        "-tumor",
                        tumorSampleName,
                        "-tumor_bam",
                        tumorBamPath,
                        "-output_dir",
                        VmDirectories.OUTPUT,
                        "-threads",
                        Bash.allCpus(),
                        "-ref_genome",
                        resourceFiles.refGenomeFile(),
                        "-loci",
                        resourceFiles.amberHeterozygousLoci())).asBash();
    }

    @Override
    public String asBash() {
        return bash;
    }
}
