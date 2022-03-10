package com.hartwig.pipeline.tertiary.amber;

import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.tertiary.HmfToolCommandBuilder;
import com.hartwig.pipeline.tools.Versions;

public class AmberCommandBuilder extends HmfToolCommandBuilder {

    private static final String JAR = "amber.jar";
    private static final String MAX_HEAP = "32G";

    private AmberCommandBuilder(final ResourceFiles resourceFiles) {
        super("amber", Versions.AMBER, MAX_HEAP, JAR);
        addArguments("-ref_genome", resourceFiles.refGenomeFile(), "-loci", resourceFiles.amberHeterozygousLoci());
    }

    public static AmberCommandBuilder newBuilder(final ResourceFiles resourceFiles) {
        return new AmberCommandBuilder(resourceFiles);
    }
}
