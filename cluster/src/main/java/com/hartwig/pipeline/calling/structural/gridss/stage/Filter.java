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
    private final String outputFilteredVcf;
    private final String outputFullVcf;

    public Filter(final String outputFilteredVcf, final String outputFullVcf) {
        super("filter", OutputFile.GZIPPED_VCF, true);
        this.outputFilteredVcf = outputFilteredVcf;
        this.outputFullVcf = outputFullVcf;
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
                new RscriptFilter(unzippedInputVcf, outputFilteredVcf, outputFullVcf),
                new MvCommand(outputFullVcf + ".bgz", outputFullVcf + ".gz"),
                new MvCommand(outputFullVcf + ".bgz.tbi", outputFullVcf + ".gz.tbi"),
                new MvCommand(outputFilteredVcf + ".bgz", outputFilteredVcf + ".gz"),
                new MvCommand(outputFilteredVcf + ".bgz.tbi", outputFilteredVcf + ".gz.tbi"));
    }
}

