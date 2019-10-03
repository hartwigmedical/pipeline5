package com.hartwig.pipeline.calling.structural.gridss.stage;

import static java.lang.String.format;
import static java.util.Arrays.asList;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.structural.gridss.command.AnnotateUntemplatedSequence;
import com.hartwig.pipeline.execution.vm.BashCommand;
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
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        String inputVcf = inputFile; //.replaceAll("\\.gz$", "");
        String inputFileNoExt = inputFile.replaceAll("\\.vcf$", "");
        String withBealn = inputFileNoExt + ".withbealn.vcf";
        String missingBealn = inputFileNoExt + ".missingbealn.vcf";
        String annotatedBealn = inputFileNoExt + ".withannotation.vcf";

        return ImmutableList.of(new GunzipAndKeepArchiveCommand(inputFile + ".gz"),
                () -> format("(grep -E '^#' %s > %s || true)", inputVcf, withBealn),
                new CpCommand(withBealn, missingBealn),
                new PipeCommands(() -> format(" (grep BEALN %s || true)", inputVcf), () -> format("(grep -vE '^#' >> %s || true) ", withBealn)),
                new PipeCommands(() -> format(" (grep -v BEALN %s || true)", inputVcf), () -> format("(grep -vE '^#' >> %s || true) ", missingBealn)),
                new AnnotateUntemplatedSequence(missingBealn, referenceGenome, annotatedBealn),
                new JavaJarCommand("picard",
                        Versions.PICARD,
                        "picard.jar",
                        "2G",
                        asList("SortVcf", "I=" + withBealn, "I=" + annotatedBealn, "O=" + output.path())));
    }
}

