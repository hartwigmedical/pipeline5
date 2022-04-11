package com.hartwig.pipeline.tertiary.linx;

import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.execution.vm.java.JavaClassCommand;
import com.hartwig.pipeline.resource.RefGenomeVersion;
import com.hartwig.pipeline.tools.Versions;

class LinxVisualisationsCommand extends JavaClassCommand {

    private static final String CIRCOS_PATH = VmDirectories.TOOLS + "/circos/" + Versions.CIRCOS + "/bin/circos";

    LinxVisualisationsCommand(final String sample, final String sampleVisDir, final RefGenomeVersion refGenomeVersion) {

        super("linx",
                Versions.LINX,
                "linx.jar",
                "com.hartwig.hmftools.linx.visualiser.SvVisualiser",
                "8G",
                "-sample",
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
                "-plot_reportable");
    }
}
