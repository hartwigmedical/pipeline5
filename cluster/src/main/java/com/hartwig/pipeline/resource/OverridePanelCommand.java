package com.hartwig.pipeline.resource;

import java.util.Collections;
import java.util.List;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.unix.MkDirCommand;

public class OverridePanelCommand {

    static final String RESOURCES_OVERRIDE = "/opt/resources/" + ResourceNames.PANEL + "/override";

    public static List<BashCommand> overrides(final Arguments arguments) {
        return arguments.panelBedLocation()
                .map(p -> List.of(new MkDirCommand(RESOURCES_OVERRIDE), copyDownPanelBed(p)))
                .orElse(Collections.emptyList());
    }

    private static BashCommand copyDownPanelBed(final String p) {
        return () -> String.format("gsutil -qm cp -n %s %s", p, RESOURCES_OVERRIDE);
    }
}