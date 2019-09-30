package com.hartwig.pipeline.calling.structural.gridss.stage;

import static java.lang.String.format;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.tools.Versions;

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
        String initialOutputPath = VmDirectories.outputFile("repeatmaster_annotation");
        String scriptDir = format("%s/gridss/%s", VmDirectories.TOOLS, Versions.GRIDSS);
        bash.addCommand(() -> format("/bin/bash -e %s/failsafe_repeatmasker_invoker.sh %s %s %s %s",
                scriptDir,
                inputFile,
                output.path(),
                repeatMaskerDb,
                scriptDir));
        // The above is a workaround as we have a failure when invoking the repeatmasker on a VCF without any real
        // data in it. When that is rectified upstream we can use these below and remove the invocation just above:
        /*
        bash.addCommand(new RscriptRepeatMasker(inputFile, output.path(), repeatMaskerDb));
        bash.addCommand(new MvCommand(initialOutputPath + ".bgz", output.path()));
        bash.addCommand(new MvCommand(initialOutputPath + ".bgz.tbi", output.path() + ".tbi"));
        */
        return bash;
    }
}
