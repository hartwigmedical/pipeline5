package com.hartwig.pipeline.tertiary.virus;

import com.hartwig.computeengine.execution.vm.VmDirectories;
import com.hartwig.pipeline.calling.command.VersionedToolCommand;
import com.hartwig.pipeline.resource.ResourceFiles;

import static com.hartwig.pipeline.resource.ResourceNames.VIRUSBREAKEND_DB;
import static com.hartwig.pipeline.tools.HmfTool.GRIDSS;

public class VirusBreakendCommand extends VersionedToolCommand {

    public VirusBreakendCommand(final ResourceFiles resourceFiles, final String tumorSample, final String tumorBamPath) {
        super(GRIDSS.getToolName(),
                "virusbreakend",
                GRIDSS.runVersion(),
                "--output",
                VmDirectories.outputFile(tumorSample + ".virusbreakend.vcf"),
                "--workingdir",
                VmDirectories.OUTPUT,
                "--reference",
                resourceFiles.refGenomeFile(),
                "--db",
                VmDirectories.resourcesPath(VIRUSBREAKEND_DB),
                "--jar",
                GRIDSS.jarPath(),
                "--gridssargs",
                "\"--jvmheap 60G\"",
                tumorBamPath);
    }
}
