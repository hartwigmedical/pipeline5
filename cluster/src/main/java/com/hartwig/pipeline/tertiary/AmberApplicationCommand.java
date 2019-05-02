package com.hartwig.pipeline.tertiary;

import com.hartwig.pipeline.execution.vm.JavaClassCommand;
import com.hartwig.pipeline.execution.vm.VmDirectories;

class AmberApplicationCommand extends JavaClassCommand {
    AmberApplicationCommand(String referenceSampleName, String referenceBamPath, String tumorSampleName, String tumorBamPath,
            String referenceGenomePath, String bedPath) {
        super("amber",
                "2.3",
                "amber.jar",
                "com.hartwig.hmftools.amber.AmberApplication",
                "32G",
                "-reference",
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
                "16",
                "-ref_genome",
                referenceGenomePath,
                "-bed",
                bedPath);
    }
}
