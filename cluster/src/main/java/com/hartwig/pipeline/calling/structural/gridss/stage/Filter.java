package com.hartwig.pipeline.calling.structural.gridss.stage;

import static java.lang.String.format;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.structural.gridss.command.BiocondaVariantAnnotationWorkaround;
import com.hartwig.pipeline.calling.structural.gridss.command.RscriptFilter;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.execution.vm.unix.MvCommand;
import com.hartwig.pipeline.execution.vm.unix.SubShellCommand;

public class Filter extends SubStage {
    private final String somaticAndQualityFilteredVcf;
    private final String somaticFilteredVcf;

    public Filter(final String somaticAndQualityFilteredVcf, final String somaticFilteredVcf) {
        super("filter", OutputFile.GZIPPED_VCF);
        this.somaticAndQualityFilteredVcf = somaticAndQualityFilteredVcf;
        this.somaticFilteredVcf = somaticFilteredVcf;
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        Matcher matcher = Pattern.compile("(.+).gz$").matcher(input.path());
        String unzippedInputVcf;
        if (matcher.matches()) {
            unzippedInputVcf = matcher.group(1);
        } else {
            throw new IllegalArgumentException(format("%s must have a .gz extension!", input.path()));
        }

        return ImmutableList.of(new SubShellCommand(new BiocondaVariantAnnotationWorkaround(input.path(), unzippedInputVcf)),
                new RscriptFilter(unzippedInputVcf, somaticAndQualityFilteredVcf, somaticFilteredVcf),
                new MvCommand(somaticFilteredVcf + ".bgz", somaticFilteredVcf + ".gz"),
                new MvCommand(somaticFilteredVcf + ".bgz.tbi", somaticFilteredVcf + ".gz.tbi"),
                new MvCommand(somaticAndQualityFilteredVcf + ".bgz", somaticAndQualityFilteredVcf + ".gz"),
                new MvCommand(somaticAndQualityFilteredVcf + ".bgz.tbi", somaticAndQualityFilteredVcf + ".gz.tbi"));
    }
}

