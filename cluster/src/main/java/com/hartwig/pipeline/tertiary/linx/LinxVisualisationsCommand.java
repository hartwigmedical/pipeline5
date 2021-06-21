package com.hartwig.pipeline.tertiary.linx;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.execution.vm.java.JavaClassCommand;
import com.hartwig.pipeline.execution.vm.java.JavaJarCommand;
import com.hartwig.pipeline.resource.RefGenomeVersion;
import com.hartwig.pipeline.tools.Versions;

class LinxVisualisationsCommand extends JavaClassCommand
{
    LinxVisualisationsCommand(final String sample, final String sampleVisDir, final RefGenomeVersion refGenomeVersion) {

    super("linx",
            Versions.LINX,
            "linx.jar",
            "com.hartwig.hmftools.linx.visualiser.SvVisualiser",
            "8G",
            "-sample",
            sample,
            "-vis_file_dir",
            sampleVisDir,
            "-plot_out",
            sampleVisDir + "/vis/",
            "-data_out",
            sampleVisDir + "/plot/",
            "-ref_genome_version",
            refGenomeVersion.linx(),
            "-plot_reportable");
    }
}
