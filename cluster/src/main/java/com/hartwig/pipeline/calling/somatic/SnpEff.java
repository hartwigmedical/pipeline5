package com.hartwig.pipeline.calling.somatic;

import com.hartwig.pipeline.execution.vm.BashStartupScript;

class SnpEff extends SubStage {

    private final String config;

    SnpEff(final String config) {
        super("snpeff.annotated", OutputFile.GZIPPED_VCF);
        this.config = config;
    }

    @Override
    BashStartupScript bash(final OutputFile input, final OutputFile output, final BashStartupScript bash) {
        String beforeZip = output.path().replace(".gz", "");
        return bash.addCommand(new SnpEffCommand(config, input.path(), beforeZip))
                .addCommand(new BgzipCommand(beforeZip))
                .addCommand(new TabixCommand(output.path()));
    }
}
