package com.hartwig.pipeline.tertiary.lilac;

import java.util.List;

import com.hartwig.pipeline.execution.vm.Bash;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.execution.vm.java.JavaJarCommand;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.tools.Versions;

public class LilacCommand extends JavaJarCommand {
    public LilacCommand(final ResourceFiles resourceFiles, final String sampleName, final String referenceBamPath, final String tumorBamPath,
            final String purpleGeneCopyNumberPath, final String purpleSomaticVariants) {
        super("lilac",
                Versions.LILAC,
                "lilac.jar",
                "15G",
                List.of("-ref_genome",
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
                resourceFiles.lilacResources(),
                "-gene_copy_number_file",
                purpleGeneCopyNumberPath,
                "-somatic_variants_file",
                purpleSomaticVariants,
                "-threads",
                Bash.allCpus()));
    }
}
