package com.hartwig.pipeline.tertiary.amber;

import com.hartwig.pipeline.execution.vm.BashCommand;
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
        bash = new TumorNormalCommand("amber",
                Versions.AMBER,
                JAR,
                MAIN_CLASS,
                MAX_HEAP,
                referenceSampleName,
                referenceBamPath,
                tumorSampleName,
                tumorBamPath,
                "-ref_genome",
                resourceFiles.refGenomeFile(),
                "-loci",
                resourceFiles.amberHeterozygousLoci()).asBash();
    }

    @Override
    public String asBash() {
        return bash;
    }
}
