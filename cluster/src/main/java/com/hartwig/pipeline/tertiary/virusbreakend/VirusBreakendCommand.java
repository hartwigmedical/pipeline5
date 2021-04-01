package com.hartwig.pipeline.tertiary.virusbreakend;

import java.util.List;

import com.google.common.collect.Lists;
import com.hartwig.pipeline.calling.command.TabixCommand;
import com.hartwig.pipeline.calling.command.VersionedToolCommand;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.tools.Versions;

public class VirusBreakendCommand implements BashCommand {

    private static final String GRIDSS = "gridss";
    private final ResourceFiles resourceFiles;
    private final String tumorSample;
    private final String tumorBamPath;
    private final String bash;

    public VirusBreakendCommand(ResourceFiles resourceFiles, String tumorSample, String tumorBamPath) {
        this.resourceFiles = resourceFiles;
        this.tumorBamPath = tumorBamPath;
        this.tumorSample = tumorSample;
        this.bash = new VersionedToolCommand(GRIDSS,
                "virusbreakend.sh",
                Versions.GRIDSS_VIRUS_BREAKEND
//                "-o",
//                output.path(),
//                "-w",
//                VmDirectories.OUTPUT,
//                "-r",
//                resourceFiles.refGenomeFile(),
//                "-j",
//                VmDirectories.TOOLS + "/" + GRIDSS + "/" + Versions.GRIDSS_VIRUS_BREAKEND + "/gridss.jar",
//                "-b",
//                resourceFiles.gridssBlacklistBed(),
//                "-c",
//                resourceFiles.gridssPropertiesFile(),
//                "--repeatmaskerbed",
//                resourceFiles.gridssRepeatMaskerDbBed(),
//                "--jvmheap",
//                "31G",
//                "--labels",
//                tumorSample,
//                tumorBamPath
        ).asBash();
    }

    @Override
    public String asBash() {
        return bash;
    }
}
