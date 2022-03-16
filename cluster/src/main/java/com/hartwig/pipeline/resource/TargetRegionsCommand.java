package com.hartwig.pipeline.resource;

import java.util.Collections;
import java.util.List;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.execution.vm.unix.MkDirCommand;

public class TargetRegionsCommand {

    static final String RESOURCES_OVERRIDE = "/opt/resources/" + ResourceNames.TARGET_REGIONS + "/override";

    public static List<BashCommand> overrides(final Arguments arguments) {
        return arguments.targetRegionsBedLocation()
                .map(p -> List.of(new MkDirCommand(RESOURCES_OVERRIDE), copyDownTargetRegionsBed(p)))
                .orElse(Collections.emptyList());
    }

    private static BashCommand copyDownTargetRegionsBed(final String p) {
        return () -> String.format("gsutil -qm cp -n %s %s", p, RESOURCES_OVERRIDE);
    }

    public static String parseTargetRegionsBedInput(final String targetRegionsBed) {
        return targetRegionsBed.startsWith("gs://")
                ? TargetRegionsCommand.RESOURCES_OVERRIDE + targetRegionsBed.substring(targetRegionsBed.lastIndexOf("/"))
                : VmDirectories.RESOURCES + "/" + ResourceNames.TARGET_REGIONS + "/" + targetRegionsBed;
    }

}