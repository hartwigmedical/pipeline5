package com.hartwig.pipeline.tertiary.amber;

import com.hartwig.pipeline.tertiary.HmfToolCommand;
import com.hartwig.pipeline.tools.Versions;

public class AmberApplicationCommand extends HmfToolCommand {
    public AmberApplicationCommand(String referenceSampleName, String referenceBamPath, String tumorSampleName, String tumorBamPath,
            String referenceGenomePath, String lociPath) {
        super("amber",
                Versions.AMBER,
                "amber.jar",
                "com.hartwig.hmftools.amber.AmberApplication",
                "32G",
                referenceSampleName,
                referenceBamPath,
                tumorSampleName,
                tumorBamPath,
                "-ref_genome",
                referenceGenomePath,
                "-loci",
                lociPath);
    }
}
