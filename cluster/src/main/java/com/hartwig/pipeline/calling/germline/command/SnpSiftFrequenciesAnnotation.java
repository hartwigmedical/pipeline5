package com.hartwig.pipeline.calling.germline.command;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.command.BgzipCommand;
import com.hartwig.pipeline.calling.command.TabixCommand;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.execution.vm.unix.SubShellCommand;

public class SnpSiftFrequenciesAnnotation extends SubStage {

    private final String frequencyDB;
    private final String snpEffConfig;

    public SnpSiftFrequenciesAnnotation(final String frequencyDB, final String snpEffConfig) {
        super("gonlv5.annotated", OutputFile.GZIPPED_VCF);
        this.frequencyDB = frequencyDB;
        this.snpEffConfig = snpEffConfig;
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        String beforeZip = output.path().replace(".gz", "");
        return ImmutableList.of(new SubShellCommand(new SnpSiftCommand("annotate",
                snpEffConfig,
                "-tabix",
                "-name",
                "GoNLv5_",
                "-info",
                "AF,AN,AC",
                frequencyDB,
                input.path(),
                ">",
                beforeZip)), new BgzipCommand(beforeZip), new TabixCommand(output.path()));
    }
}