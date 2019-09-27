package com.hartwig.pipeline.calling.structural.gridss.stage;

import static java.lang.String.format;
import static java.util.Arrays.asList;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.structural.gridss.command.AnnotateUntemplatedSequence;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.JavaJarCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.execution.vm.unix.CpCommand;
import com.hartwig.pipeline.execution.vm.unix.GunzipAndKeepArchiveCommand;
import com.hartwig.pipeline.execution.vm.unix.PipeCommands;
import com.hartwig.pipeline.tools.Versions;

public class ViralAnnotation extends SubStage {

    private final String referenceGenome;
    private final String inputFile;

    public ViralAnnotation(final String referenceGenome, final String inputFile) {
        super("viral_annotation", OutputFile.VCF, false);
        this.referenceGenome = referenceGenome;
        this.inputFile = inputFile;
    }

    @Override
    public BashStartupScript bash(final OutputFile input, final OutputFile output, final BashStartupScript bash) {
        String inputVcf = inputFile; //.replaceAll("\\.gz$", "");
        String inputFileNoExt = inputFile.replaceAll("\\.vcf$", "");
        String withBealn = inputFileNoExt + ".withbealn.vcf";
        String missingBealn = inputFileNoExt + ".missingbealn.vcf";
        String annotatedBealn = inputFileNoExt + ".withannotation.vcf";

        return bash.addCommand(new GunzipAndKeepArchiveCommand(inputFile + ".gz"))
                .addCommand(() -> format("(grep -E '^#' %s > %s || true)", inputVcf, withBealn))
                .addCommand(new CpCommand(withBealn, missingBealn))
                .addCommand(new PipeCommands(() -> format(" (grep BEALN %s || true)", inputVcf),
                        () -> format("(grep -vE '^#' >> %s || true) ", withBealn)))
                .addCommand(new PipeCommands(() -> format(" (grep -v BEALN %s || true)", inputVcf),
                        () -> format("(grep -vE '^#' >> %s || true) ", missingBealn)))
                .addCommand(new AnnotateUntemplatedSequence(missingBealn, referenceGenome, annotatedBealn))
                .addCommand(new JavaJarCommand("picard",
                        Versions.PICARD,
                        "picard.jar",
                        "2G",
                        asList("SortVcf", "I=" + withBealn, "I=" + annotatedBealn, "O=" + output.path())));
    }
}

