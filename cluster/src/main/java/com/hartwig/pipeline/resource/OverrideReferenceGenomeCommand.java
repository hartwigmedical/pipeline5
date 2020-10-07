package com.hartwig.pipeline.resource;

import java.util.Collections;
import java.util.List;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.unix.MkDirCommand;

public class OverrideReferenceGenomeCommand {

    static final String RESOURCES_OVERRIDE = "/opt/resources/" + ResourceNames.REFERENCE_GENOME + "/override";

    public static List<BashCommand> overrides(final Arguments arguments) {
        return arguments.refGenomeUrl()
                .map(p -> List.of(new MkDirCommand(RESOURCES_OVERRIDE), copyDownReferenceGenome(p)))
                .orElse(Collections.emptyList());
    }

    private static BashCommand copyDownReferenceGenome(final String p) {
        return () -> "gsutil -qm cp -n " + p.substring(0, p.lastIndexOf('.')) + "* " + RESOURCES_OVERRIDE;
    }
}
