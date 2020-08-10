package com.hartwig.pipeline.tertiary.bachelor;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.execution.vm.JavaJarCommand;
import com.hartwig.pipeline.tools.Versions;

class BachelorCommand extends JavaJarCommand {
    BachelorCommand(final String tumorSampleName, final String germlineVcfPath, final String tumorBamPath, final String purpleDir,
            final String bachelorXmlConfigPath, final String clinVarFiltersPath, final String refGenomePath, final String outputDir) {
        super("bachelor",
                Versions.BACHELOR,
                "bachelor.jar",
                "8G",
                ImmutableList.<String>builder().add("-sample",
                        tumorSampleName,
                        "-germline_vcf",
                        germlineVcfPath,
                        "-tumor_bam_file",
                        tumorBamPath,
                        "-purple_data_dir",
                        purpleDir,
                        "-xml_config",
                        bachelorXmlConfigPath,
                        "-ext_filter_file",
                        clinVarFiltersPath,
                        "-ref_genome",
                        refGenomePath,
                        "-include_vcf_filtered",
                        "-output_dir",
                        outputDir,
                        "-log_debug").build());
    }
}
