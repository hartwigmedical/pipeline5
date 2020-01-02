package com.hartwig.pipeline.tertiary.cobalt;

import com.hartwig.pipeline.tertiary.HmfToolCommand;
import com.hartwig.pipeline.tools.Versions;

class CobaltApplicationCommand extends HmfToolCommand {
    CobaltApplicationCommand(String referenceSampleName, String referenceBamPath, String tumorSampleName, String tumorBamPath,
            String gcProfileBed) {
        super("cobalt",
                Versions.COBALT,
                "cobalt.jar",
                "com.hartwig.hmftools.cobalt.CountBamLinesApplication",
                "8G",
                referenceSampleName,
                referenceBamPath,
                tumorSampleName,
                tumorBamPath,
                "-gc_profile",
                gcProfileBed);
    }
}
