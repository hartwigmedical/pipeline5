package com.hartwig.pipeline.calling.structural.gridss.stage;

import java.util.List;

import com.google.common.collect.Lists;
import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.command.TabixCommand;
import com.hartwig.pipeline.calling.command.VersionedToolCommand;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.tools.Versions;

public class Driver extends SubStage {

    private static final String GRIDSS = "gridss";
    private final String assemblyBamPath;
    private final String referenceGenomePath;
    private final String blacklistBedPath;
    private final String gridssConfigPath;
    private final String repeatMaskerDbBed;
    private final String referenceBamPath;
    private final String tumorBamPath;

    public Driver(final ResourceFiles resourceFiles, final String assemblyBamPath, final String referenceBamPath, final String tumorBamPath) {
        super("gridss.driver", OutputFile.GZIPPED_VCF);
        this.assemblyBamPath = assemblyBamPath;
        this.referenceGenomePath =  resourceFiles.refGenomeFile();
        this.blacklistBedPath = resourceFiles.gridssBlacklistBed();
        this.gridssConfigPath = resourceFiles.gridssPropertiesFile();
        this.repeatMaskerDbBed = resourceFiles.gridssRepeatMaskerDbBed();
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
                referenceGenomePath,
                "-j",
                VmDirectories.TOOLS + "/" + GRIDSS + "/" + Versions.GRIDSS + "/gridss.jar",
                "-b",
                blacklistBedPath,
                "-c",
                gridssConfigPath,
                "--repeatmaskerbed",
                repeatMaskerDbBed,
                "--jvmheap",
                "31G",
                referenceBamPath,
                tumorBamPath);

        BashCommand index = new TabixCommand(output.path());
        return Lists.newArrayList(gridss, index);
    }
}
