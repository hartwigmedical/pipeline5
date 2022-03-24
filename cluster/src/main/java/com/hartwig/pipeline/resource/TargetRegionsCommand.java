package com.hartwig.pipeline.resource;

import java.util.Collections;
import java.util.List;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.unix.MkDirCommand;

public class TargetRegionsCommand {

    public static final String OVERRIDE_SUBDIR = "/override";
    static final String RESOURCES_OVERRIDE = "/opt/resources/" + ResourceNames.TARGET_REGIONS + OVERRIDE_SUBDIR;

    public static List<BashCommand> overrides(final Arguments arguments) {
        return arguments.targetRegionsBedLocation()
                .map(p -> List.of(new MkDirCommand(RESOURCES_OVERRIDE), copyDownTargetRegionsBed(p)))
                .orElse(Collections.emptyList());
    }

    private static BashCommand copyDownTargetRegionsBed(final String p) {
        return () -> String.format("gsutil -qm cp -n %s %s", p, RESOURCES_OVERRIDE);
    }
}