package com.hartwig.pipeline.tertiary.virus;

import static com.hartwig.pipeline.resource.ResourceNames.VIRUSBREAKEND_DB;
import static com.hartwig.pipeline.tools.HmfTool.VIRUSBREAKEND_GRIDSS;

import com.hartwig.computeengine.execution.vm.VmDirectories;
import com.hartwig.pipeline.calling.command.VersionedToolCommand;
import com.hartwig.pipeline.resource.ResourceFiles;

public class VirusBreakendCommand extends VersionedToolCommand {

    // For now, move the `HmfTool.GRIDSS` definition here. Virusbreakend will be rewritten in the near future, so the gridss dependency will
    // no longer be needed
    private static final String GRIDSS_TOOL_NAME = "gridss";
    private static final String GRIDSS_JAR_PATH =  String.format("%s/%s/%s/%s.jar", VmDirectories.TOOLS, GRIDSS_TOOL_NAME, VIRUSBREAKEND_GRIDSS.runVersion(), GRIDSS_TOOL_NAME);

    public VirusBreakendCommand(final ResourceFiles resourceFiles, final String tumorSample, final String tumorBamPath) {

        super(GRIDSS_TOOL_NAME,
                "virusbreakend",
                VIRUSBREAKEND_GRIDSS.runVersion(),
                "--output",
                VmDirectories.outputFile(tumorSample + ".virusbreakend.vcf"),
                "--workingdir",
                VmDirectories.OUTPUT,
                "--reference",
                resourceFiles.refGenomeFile(),
                "--db",
                VmDirectories.resourcesPath(VIRUSBREAKEND_DB),
                "--jar",
                GRIDSS_JAR_PATH,
                "--gridssargs",
                "\"--jvmheap 60G\"",
                tumorBamPath);
    }
}
