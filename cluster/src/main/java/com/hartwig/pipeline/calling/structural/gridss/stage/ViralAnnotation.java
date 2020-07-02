package com.hartwig.pipeline.calling.structural.gridss.stage;

import static java.lang.String.format;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.structural.gridss.command.AnnotateInsertedSequence;
import com.hartwig.pipeline.calling.structural.gridss.command.SortVcf;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.execution.vm.unix.CpCommand;
import com.hartwig.pipeline.execution.vm.unix.GunzipAndKeepArchiveCommand;
import com.hartwig.pipeline.execution.vm.unix.PipeCommands;

public class ViralAnnotation extends SubStage {
    private final String referenceGenome;

    public ViralAnnotation(final String referenceGenome) {
        super("viral_annotation", OutputFile.GZIPPED_VCF, false);
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
                new PipeCommands(() -> format(" (grep BEALN %s || true)", inputVcfGunzipped),
                        () -> format("(grep -vE '^#' >> %s || true) ", withBealn)),
                new PipeCommands(() -> format(" (grep -v BEALN %s || true)", inputVcfGunzipped),
                        () -> format("(grep -vE '^#' >> %s || true) ", missingBealn)),
                AnnotateInsertedSequence.viralAnnotation(missingBealn, annotatedBealn, referenceGenome),
                new SortVcf(withBealn, annotatedBealn, output.path()));
    }
}

