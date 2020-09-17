package com.hartwig.pipeline.calling.structural.gridss.stage;

import java.util.List;

import com.google.common.collect.Lists;
import com.hartwig.pipeline.calling.command.TabixCommand;
import com.hartwig.pipeline.calling.command.VersionedToolCommand;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.SubStage;
import com.hartwig.pipeline.tools.Versions;

public class Driver extends SubStage {

    private static final String GRIDSS = "gridss";
    private final ResourceFiles resourceFiles;
    private final String assemblyBamPath;
    private final String tumorBamPath;
    private final String referenceBamPath;

    public Driver(final ResourceFiles resourceFiles, final String assemblyBamPath, final String referenceBamPath, final String tumorBamPath) {
        super("gridss.driver", FileTypes.GZIPPED_VCF);
        this.resourceFiles = resourceFiles;
        this.assemblyBamPath = assemblyBamPath;
        this.referenceBamPath = referenceBamPath;
        this.tumorBamPath = tumorBamPath;
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        BashCommand gridss = new VersionedToolCommand(GRIDSS,
                "gridss.sh",
                Versions.GRIDSS,
                "-o",
                output.path(),
                "-a",
                assemblyBamPath,
                "-w",
                VmDirectories.OUTPUT,
                "-r",
                resourceFiles.refGenomeFile(),
                "-j",
                VmDirectories.TOOLS + "/" + GRIDSS + "/" + Versions.GRIDSS + "/gridss.jar",
                "-b",
                resourceFiles.gridssBlacklistBed(),
                "-c",
                resourceFiles.gridssPropertiesFile(),
                "--repeatmaskerbed",
                resourceFiles.gridssRepeatMaskerDbBed(),
                "--jvmheap",
                "31G",
                referenceBamPath,
                tumorBamPath);

        BashCommand index = new TabixCommand(output.path());
        return Lists.newArrayList(gridss, index);
    }
}
