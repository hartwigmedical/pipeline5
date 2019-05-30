package com.hartwig.pipeline.calling.somatic;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.command.BgzipCommand;
import com.hartwig.pipeline.calling.command.TabixCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.OutputFile;

class SnpEff extends SubStage {

    private final String config;

    SnpEff(final String config) {
        super("snpeff.annotated", OutputFile.GZIPPED_VCF);
        this.config = config;
    }

    @Override
    public BashStartupScript bash(final OutputFile input, final OutputFile output, final BashStartupScript bash) {
        String beforeZip = output.path().replace(".gz", "");
        return bash.addCommand(new SnpEffCommand(config, input.path(), beforeZip))
                .addCommand(new BgzipCommand(beforeZip))
                .addCommand(new TabixCommand(output.path()));
    }
}
