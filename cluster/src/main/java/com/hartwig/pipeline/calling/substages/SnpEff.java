package com.hartwig.pipeline.calling.substages;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.command.BgzipCommand;
import com.hartwig.pipeline.calling.command.TabixCommand;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;

public class SnpEff extends SubStage {

    private final String config;

    public SnpEff(final String config) {
        super("snpeff.annotated", OutputFile.GZIPPED_VCF);
        this.config = config;
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        String beforeZip = output.path().replace(".gz", "");
        return ImmutableList.of(new SnpEffCommand(config, input.path(), beforeZip),
                new BgzipCommand(beforeZip),
                new TabixCommand(output.path()));
    }
}
