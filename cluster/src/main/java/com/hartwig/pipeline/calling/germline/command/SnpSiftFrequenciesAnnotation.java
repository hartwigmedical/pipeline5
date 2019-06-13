package com.hartwig.pipeline.calling.germline.command;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.command.BgzipCommand;
import com.hartwig.pipeline.calling.command.TabixCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
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
    public BashStartupScript bash(final OutputFile input, final OutputFile output, final BashStartupScript bash) {
        String beforeZip = output.path().replace(".gz", "");
        return bash.addCommand(new SubShellCommand(new SnpSiftCommand("annotate", "-tabix",
                "-name",
                "GoNLv5",
                "-info",
                "AF,AN,AC",
                frequencyDB,
                input.path(),
                ">",
                beforeZip))).addCommand(new BgzipCommand(beforeZip)).addCommand(new TabixCommand(output.path()));
    }
}