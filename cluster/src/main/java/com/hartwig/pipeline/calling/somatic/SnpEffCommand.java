package com.hartwig.pipeline.calling.somatic;

import com.google.common.collect.Lists;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.JavaJarCommand;

class SnpEffCommand extends JavaJarCommand {

    SnpEffCommand(final String config, final String inputVcf, final String outputVcf) {
        super("snpEff",
                "4.3s",
                "snpEff.jar",
                "20G",
                Lists.newArrayList("-c",
                        config,
                        "GRCh37.75",
                        "-v",
                        inputVcf,
                        "-hgvs",
                        "-lof",
                        "-no-downstream",
                        "-ud",
                        "1000",
                        "-no-intergenic",
                        "-noShiftHgvs",
                        ">",
                        outputVcf));
    }
}
