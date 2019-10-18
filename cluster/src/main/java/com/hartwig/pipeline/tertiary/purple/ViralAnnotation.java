package com.hartwig.pipeline.tertiary.purple;

import static java.lang.String.format;
import static java.util.Arrays.asList;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.structural.gridss.command.AnnotateUntemplatedSequence;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.JavaJarCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.execution.vm.unix.CpCommand;
import com.hartwig.pipeline.execution.vm.unix.GunzipAndKeepArchiveCommand;
import com.hartwig.pipeline.execution.vm.unix.PipeCommands;
import com.hartwig.pipeline.tools.Versions;

public class ViralAnnotation extends SubStage {

    private final String referenceGenome;

    ViralAnnotation(final String referenceGenome) {
        super("purple.viral_annotation", OutputFile.GZIPPED_VCF, false);
        this.referenceGenome = referenceGenome;
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        String inputVcf = input.path();
        String inputVcfGunzipped = inputVcf.replaceAll("\\.gz$", "");
        String inputFileNoExt = inputVcfGunzipped.replaceAll("\\.vcf$", "");
        String withBealn = inputFileNoExt + ".withbealn.vcf";
        String missingBealn = inputFileNoExt + ".missingbealn.vcf";
        String annotatedBealn = inputFileNoExt + ".withannotation.vcf";

        return ImmutableList.of(new GunzipAndKeepArchiveCommand(input.path()),
                () -> format("(grep -E '^#' %s > %s || true)", inputVcfGunzipped, withBealn),
                new CpCommand(withBealn, missingBealn),
                new PipeCommands(() -> format(" (grep BEALN %s || true)", inputVcfGunzipped), () -> format("(grep -vE '^#' >> %s || true) ", withBealn)),
                new PipeCommands(() -> format(" (grep -v BEALN %s || true)", inputVcfGunzipped), () -> format("(grep -vE '^#' >> %s || true) ", missingBealn)),
                new AnnotateUntemplatedSequence(missingBealn, referenceGenome, annotatedBealn),
                new JavaJarCommand("picard",
                        Versions.PICARD,
                        "picard.jar",
                        "2G",
                        asList("SortVcf", "I=" + withBealn, "I=" + annotatedBealn, "O=" + output.path())));
    }
}

