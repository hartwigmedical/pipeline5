package com.hartwig.pipeline.tertiary.cobalt;

import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.tertiary.HmfToolCommandBuilder;
import com.hartwig.pipeline.tools.Versions;

public class CobaltCommandBuilder extends HmfToolCommandBuilder {

    private static final String JAR = "cobalt.jar";
    private static final String MAX_HEAP = "8G";
    private static final String MAIN_CLASS = "com.hartwig.hmftools.cobalt.CountBamLinesApplication";
    private static final String COBALT = "cobalt";

    public CobaltCommandBuilder(final ResourceFiles resourceFiles) {
        super(COBALT, Versions.COBALT, MAX_HEAP, JAR);
        addArguments("-ref_genome", resourceFiles.refGenomeFile(), "-gc_profile", resourceFiles.gcProfileFile());
    }

    public static CobaltCommandBuilder newBuilder(final ResourceFiles resourceFiles) {
        return new CobaltCommandBuilder(resourceFiles);
    }
}
