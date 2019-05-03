package com.hartwig.pipeline.tertiary.amber;

import com.hartwig.pipeline.tertiary.HmfToolCommand;

class AmberApplicationCommand extends HmfToolCommand {
    AmberApplicationCommand(String referenceSampleName, String referenceBamPath, String tumorSampleName, String tumorBamPath,
            String referenceGenomePath, String bedPath) {
        super("amber",
                "2.3",
                "amber.jar",
                "com.hartwig.hmftools.amber.AmberApplication",
                "32G",
                referenceSampleName,
                referenceBamPath,
                tumorSampleName,
                tumorBamPath,
                "-ref_genome",
                referenceGenomePath,
                "-bed",
                bedPath);
    }
}
