package com.hartwig.pipeline.tertiary.lilac;

import com.hartwig.pipeline.execution.vm.Bash;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.JavaClassCommand;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.tools.Versions;

public class LilacApplicationCommand implements BashCommand {

    private static final String JAR = "lilac.jar";
    private static final String MAX_HEAP = "15G";
    private static final String MAIN_CLASS = "com.hartwig.hmftools.lilac.LilacApplicationKt";

    private final String bash;

    public LilacApplicationCommand(ResourceFiles resourceFiles, String sampleName, String referenceBamPath, String tumorBamPath,
            String purpleGeneCopyNumberPath, String purpleSomaticVariants) {
        bash = new JavaClassCommand("lilac",
                Versions.LILAC,
                JAR,
                MAIN_CLASS,
                MAX_HEAP,
                "-ref_genome",
                resourceFiles.refGenomeFile(),
                "-sample",
                sampleName,
                "-reference_bam",
                referenceBamPath,
                "-tumor_bam",
                tumorBamPath,
                "-output_dir",
                VmDirectories.OUTPUT,
                "-resource_dir",
                resourceFiles.lilacHlaSequences(),
                "-gene_copy_number",
                purpleGeneCopyNumberPath,
                "-somatic_vcf",
                purpleSomaticVariants,
                "-threads",
                Bash.allCpus()).asBash();
    }

    @Override
    public String asBash() {
        return bash;
    }
}
