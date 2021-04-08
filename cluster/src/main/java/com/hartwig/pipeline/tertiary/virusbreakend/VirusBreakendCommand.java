package com.hartwig.pipeline.tertiary.virusbreakend;

import com.hartwig.pipeline.calling.command.VersionedToolCommand;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.tools.Versions;
import static com.hartwig.pipeline.resource.ResourceNames.VIRUSBREAKEND_DB;

public class VirusBreakendCommand implements BashCommand {

    private final String bash;

    public VirusBreakendCommand(ResourceFiles resourceFiles, String tumorSample, String tumorBamPath) {
        this.bash = new VersionedToolCommand("gridss",
                "virusbreakend.sh",
                Versions.GRIDSS_VIRUS_BREAKEND,
                "-o",
                tumorSample + ".virusbreakend.vcf",
                "-w",
                VmDirectories.OUTPUT,
                "-r",
                resourceFiles.refGenomeFile(),
                "--db",
                VmDirectories.RESOURCES + "/virusbreakend/" + VIRUSBREAKEND_DB,
                "-j",
                VmDirectories.TOOLS + "/gridss/" + Versions.GRIDSS_VIRUS_BREAKEND + "/gridss.jar",
                tumorBamPath
        ).asBash();
    }

    @Override
    public String asBash() {
        return bash;
    }
}
