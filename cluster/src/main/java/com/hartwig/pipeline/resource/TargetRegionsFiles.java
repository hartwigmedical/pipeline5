package com.hartwig.pipeline.resource;

import java.util.Collections;
import java.util.List;

import com.google.api.client.util.Lists;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.CommonArguments;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.execution.vm.unix.MkDirCommand;

public class TargetRegionsFiles
{
    private static final String TARGETED_REGIONS_DIR = String.format("%s/%s", VmDirectories.RESOURCES, ResourceNames.TARGET_REGIONS);

    private static final String OVERRIDE_TARGETED_REGIONS_DIR = String.format("%s/override", TARGETED_REGIONS_DIR);

    public static List<BashCommand> overrides(final Arguments arguments) {

        List<BashCommand> commands = Lists.newArrayList();

        if(arguments.targetRegionsDir().isEmpty())
            return commands;

        commands.add(new MkDirCommand(OVERRIDE_TARGETED_REGIONS_DIR));

        String targetRegionsDir = arguments.targetRegionsDir().get();
        commands.add(() -> String.format("gsutil -qm cp -n %s/* %s", targetRegionsDir, OVERRIDE_TARGETED_REGIONS_DIR));
        return commands;
    }

    public static void parseArguments(final ResourceFiles resourceFiles, final CommonArguments arguments) {
        if (arguments.useTargetRegions()) {
            resourceFiles.setTargetRegionsDir(TARGETED_REGIONS_DIR);
        }
        else if(arguments.targetRegionsDir().isPresent()) {
            resourceFiles.setTargetRegionsDir(OVERRIDE_TARGETED_REGIONS_DIR);
        }
    }

    private static String parseTargetRegionsDir(final String targetRegionsDir) {
        return targetRegionsDir.startsWith("gs://")
                ? TargetRegionsFiles.OVERRIDE_TARGETED_REGIONS_DIR + targetRegionsDir.substring(targetRegionsDir.lastIndexOf("/"))
                : VmDirectories.RESOURCES + "/" + ResourceNames.TARGET_REGIONS + "/" + targetRegionsDir;
    }

}