package com.hartwig.pipeline.calling.somatic;

import com.hartwig.pipeline.execution.vm.GatkCommand;

class CombineVcfsCommand extends GatkCommand {

    private static final String PIPELINE_V4_HEAP_SETTING = "20G";

    CombineVcfsCommand(String referenceGenomePath, String snvVcfPath, String indelsVcfPath, String outputPath) {
        super(PIPELINE_V4_HEAP_SETTING,
                "CombineVariants",
                "-R",
                referenceGenomePath,
                "--genotypemergeoption",
                "unsorted",
                "-V:snvs",
                snvVcfPath,
                "-V:indels",
                indelsVcfPath,
                "-o",
                outputPath);
    }
}
