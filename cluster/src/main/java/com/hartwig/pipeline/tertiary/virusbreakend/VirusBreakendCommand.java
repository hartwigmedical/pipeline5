package com.hartwig.pipeline.tertiary.virusbreakend;

import com.hartwig.pipeline.calling.command.VersionedToolCommand;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.tools.Versions;
import static com.hartwig.pipeline.resource.ResourceNames.VIRUSBREAKEND_DB;

public class VirusBreakendCommand extends VersionedToolCommand {

    public VirusBreakendCommand(ResourceFiles resourceFiles, String tumorSample, String tumorBamPath) {
        super("gridss",
                "virusbreakend.sh",
                Versions.GRIDSS_VIRUS_BREAKEND,
                "--output",
                VmDirectories.outputFile( tumorSample + ".virusbreakend.vcf"),
                "--workingdir",
                VmDirectories.OUTPUT,
                "--reference",
                resourceFiles.refGenomeFile(),
                "--db",
                VmDirectories.resourcesPath("virusbreakend/" + VIRUSBREAKEND_DB),
                "--jar",
                VmDirectories.toolPath("gridss/" + Versions.GRIDSS_VIRUS_BREAKEND + "/gridss.jar"),
                tumorBamPath
        );
    }
}
