package com.hartwig.pipeline.calling.substages;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.command.BgzipCommand;
import com.hartwig.pipeline.calling.command.TabixCommand;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.resource.ResourceFiles;

public class SnpEff extends SubStage {

    private final String config;
    private final ResourceFiles resourceFiles;

    public SnpEff(final String config, final ResourceFiles resourceFiles) {
        super("snpeff.annotated", OutputFile.GZIPPED_VCF);
        this.config = config;
        this.resourceFiles = resourceFiles;
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        String beforeZip = output.path().replace(".gz", "");

        return ImmutableList.of(new SnpEffCommand(config, input.path(), beforeZip, resourceFiles.snpEffVersion()),
                new BgzipCommand(beforeZip),
                new TabixCommand(output.path()));
    }
}
