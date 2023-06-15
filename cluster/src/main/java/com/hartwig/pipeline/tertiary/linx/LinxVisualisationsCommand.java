package com.hartwig.pipeline.tertiary.linx;

import java.util.List;

import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.execution.vm.java.JavaClassCommand;
import com.hartwig.pipeline.resource.RefGenomeVersion;
import com.hartwig.pipeline.tools.ToolInfo;
import com.hartwig.pipeline.tools.Versions;

class LinxVisualisationsCommand extends JavaClassCommand {

    private static final String CIRCOS_PATH = VmDirectories.TOOLS + "/circos/" + Versions.CIRCOS + "/bin/circos";
    public static final String LINX_VISUALISER = "com.hartwig.hmftools.linx.visualiser.SvVisualiser";

    LinxVisualisationsCommand(final String sample, final String sampleVisDir, final RefGenomeVersion refGenomeVersion) {

        super(ToolInfo.LINX, LINX_VISUALISER,
                List.of("-sample",
                sample,
                "-ref_genome_version",
                refGenomeVersion.toString(),
                "-circos",
                CIRCOS_PATH,
                "-vis_file_dir",
                sampleVisDir,
                "-data_out",
                sampleVisDir + "/circos/",
                "-plot_out",
                sampleVisDir + "/plot/",
                "-plot_reportable"));
    }

    private static List<String> buildArguments()
    {
        return List.of();
    }
}
