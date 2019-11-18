package com.hartwig.pipeline.calling.structural.gridss.stage;

import java.util.Collections;
import java.util.List;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.command.VersionedToolCommand;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.tools.Versions;

public class Driver extends SubStage {

    private static final String GRIDSS = "gridss";
    private final String assemblyBamPath;
    private final String referenceGenomePath;
    private final String blacklistBedPath;
    private final String gridssConfigPath;
    private final String referenceBamPath;
    private final String tumorBamPath;

    public Driver(final String assemblyBamPath, final String referenceGenomePath, final String blacklistBedPath,
            final String gridssConfigPath, final String referenceBamPath, final String tumorBamPath) {
        super("gridss.somatic.full", OutputFile.VCF);
        this.assemblyBamPath = assemblyBamPath;
        this.referenceGenomePath = referenceGenomePath;
        this.blacklistBedPath = blacklistBedPath;
        this.gridssConfigPath = gridssConfigPath;
        this.referenceBamPath = referenceBamPath;
        this.tumorBamPath = tumorBamPath;
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        return Collections.singletonList(new VersionedToolCommand(GRIDSS,
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
                referenceBamPath,
                tumorBamPath));
    }
}
