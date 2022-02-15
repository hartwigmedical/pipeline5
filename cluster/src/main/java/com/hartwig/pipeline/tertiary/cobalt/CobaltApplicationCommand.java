package com.hartwig.pipeline.tertiary.cobalt;

import com.hartwig.pipeline.GermlineOnlyCommand;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.tertiary.TumorNormalCommand;
import com.hartwig.pipeline.tertiary.TumorOnlyCommand;
import com.hartwig.pipeline.tools.Versions;

public class CobaltApplicationCommand {

    private static final String JAR = "cobalt.jar";
    private static final String MAX_HEAP = "8G";
    private static final String MAIN_CLASS = "com.hartwig.hmftools.cobalt.CountBamLinesApplication";
    private static final String COBALT = "cobalt";

    static BashCommand tumorOnly(ResourceFiles resourceFiles, String tumorSampleName, String tumorBamPath) {
        return new TumorOnlyCommand(COBALT,
                Versions.COBALT,
                JAR,
                MAIN_CLASS,
                MAX_HEAP,
                tumorSampleName,
                tumorBamPath,
                "-ref_genome",
                resourceFiles.refGenomeFile(),
                "-tumor_only_diploid_bed",
                resourceFiles.diploidRegionsBed(),
                "-gc_profile",
                resourceFiles.gcProfileFile());
    }

    static BashCommand somatic(ResourceFiles resourceFiles, String referenceSampleName, String referenceBamPath, String tumorSampleName,
            String tumorBamPath) {
        return new TumorNormalCommand(COBALT,
                Versions.COBALT,
                JAR,
                MAIN_CLASS,
                MAX_HEAP,
                referenceSampleName,
                referenceBamPath,
                tumorSampleName,
                tumorBamPath,
                "-ref_genome",
                resourceFiles.refGenomeFile(),
                "-gc_profile",
                resourceFiles.gcProfileFile());
    }

    public static BashCommand germlineOnly(final ResourceFiles resourceFiles, final String sampleName, final String referenceBamPath) {
        return new GermlineOnlyCommand(COBALT,
                Versions.COBALT,
                JAR,
                MAIN_CLASS,
                MAX_HEAP,
                sampleName,
                referenceBamPath,
                "-ref_genome",
                resourceFiles.refGenomeFile(),
                "-gc_profile",
                resourceFiles.gcProfileFile());
    }
}