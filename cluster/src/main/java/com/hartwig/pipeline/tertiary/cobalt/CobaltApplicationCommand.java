package com.hartwig.pipeline.tertiary.cobalt;

import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.tertiary.TumorNormalCommand;
import com.hartwig.pipeline.tertiary.TumorOnlyCommand;
import com.hartwig.pipeline.tools.Versions;

public class CobaltApplicationCommand implements BashCommand {

    private static final String JAR = "cobalt.jar";
    private static final String MAX_HEAP = "8G";
    private static final String MAIN_CLASS = "com.hartwig.hmftools.cobalt.CountBamLinesApplication";

    private final String bash;

    public CobaltApplicationCommand(ResourceFiles resourceFiles, String tumorSampleName, String tumorBamPath) {
        bash = new TumorOnlyCommand("cobalt",
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
                resourceFiles.gcProfileFile()).asBash();
    }

    public CobaltApplicationCommand(ResourceFiles resourceFiles, String referenceSampleName, String referenceBamPath, String tumorSampleName,
            String tumorBamPath) {
        bash = new TumorNormalCommand("cobalt",
                Versions.COBALT,
                JAR,
                MAIN_CLASS,
                MAX_HEAP,
                referenceSampleName,
                referenceBamPath,
                tumorSampleName,
                tumorBamPath,
                "-gc_profile",
                resourceFiles.gcProfileFile()).asBash();
    }

    @Override
    public String asBash() {
        return bash;
    }
}