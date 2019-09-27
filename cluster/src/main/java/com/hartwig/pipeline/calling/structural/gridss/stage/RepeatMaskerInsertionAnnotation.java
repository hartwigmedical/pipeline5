package com.hartwig.pipeline.calling.structural.gridss.stage;

import static java.lang.String.format;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.structural.gridss.command.RscriptRepeatMasker;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.execution.vm.unix.MvCommand;

public class RepeatMaskerInsertionAnnotation extends SubStage {
    private final String repeatMaskerDb;
    private final String inputFile;

    public RepeatMaskerInsertionAnnotation(final String repeatMaskerDb, final String inputFile) {
        super("repeatmaster_annotation", OutputFile.GZIPPED_VCF);
        this.repeatMaskerDb = repeatMaskerDb;
        this.inputFile = inputFile;
    }

    @Override
    public BashStartupScript bash(final OutputFile input, final OutputFile output, final BashStartupScript bash) {
        String initialOutputPath = format("%s/repeatmaster_annotation", VmDirectories.OUTPUT);
        bash.addCommand(new RscriptRepeatMasker(inputFile, output.path(), repeatMaskerDb));
        bash.addCommand(new MvCommand(initialOutputPath + ".bgz", output.fileName()));
        bash.addCommand(new MvCommand(initialOutputPath + ".bgz.tbi", output.path() + ".tbi"));
        return bash;
    }
}
