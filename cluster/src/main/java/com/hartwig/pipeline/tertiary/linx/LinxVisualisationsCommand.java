package com.hartwig.pipeline.tertiary.linx;

import static com.hartwig.pipeline.tools.ExternalTool.CIRCOS;

import java.util.List;

import com.hartwig.pipeline.execution.vm.java.JavaClassCommand;
import com.hartwig.pipeline.resource.RefGenomeVersion;
import com.hartwig.pipeline.tools.HmfTool;

class LinxVisualisationsCommand extends JavaClassCommand {

    public static final String LINX_VISUALISER = "com.hartwig.hmftools.linx.visualiser.SvVisualiser";

    LinxVisualisationsCommand(final String sample, final String sampleVisDir, final RefGenomeVersion refGenomeVersion) {

        super(HmfTool.LINX,
                LINX_VISUALISER,
                List.of("-sample",
                        sample,
                        "-ref_genome_version",
                        refGenomeVersion.toString(),
                        "-circos",
                        CIRCOS.binaryPath(),
                        "-vis_file_dir",
                        sampleVisDir,
                        "-data_out",
                        sampleVisDir + "/circos/",
                        "-plot_out",
                        sampleVisDir + "/plot/",
                        "-plot_reportable"));
    }
}
