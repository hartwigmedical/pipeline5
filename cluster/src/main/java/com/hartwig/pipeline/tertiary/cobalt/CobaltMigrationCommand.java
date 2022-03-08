package com.hartwig.pipeline.tertiary.cobalt;

import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.execution.vm.java.JavaClassCommand;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.tools.Versions;

public class CobaltMigrationCommand implements BashCommand {

    private final String bash;

    public CobaltMigrationCommand(final ResourceFiles resourceFiles, final String reference, final String tumor) {
        bash = new JavaClassCommand("cobalt",
                Versions.COBALT,
                "cobalt.jar",
                "com.hartwig.hmftools.cobalt.CountBamLinesMigration",
                "8G",
                "-reference",
                reference,
                "-tumor",
                tumor,
                "-gc_profile",
                resourceFiles.gcProfileFile(),
                "-input_dir",
                VmDirectories.INPUT,
                "-output_dir",
                VmDirectories.OUTPUT).asBash();
    }

    @Override
    public String asBash() {
        return bash;
    }
}
