package com.hartwig.pipeline.calling.structural.gridss.stage;

import static java.lang.String.format;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.structural.gridss.command.BiocondaVariantAnnotationWorkaround;
import com.hartwig.pipeline.calling.structural.gridss.command.RscriptFilter;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.execution.vm.VmDirectories;
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
    public BashStartupScript bash(final OutputFile input, final OutputFile output, final BashStartupScript bash) {
        Matcher matcher = Pattern.compile("(.+).gz$").matcher(input.path());
        String unzippedInputVcf;
        if (matcher.matches()) {
            unzippedInputVcf = matcher.group(1);
        }
        else {
            throw new IllegalArgumentException(format("%s must have a .gz extension!", input.path()));
        }

        bash.addCommand(new SubShellCommand(new BiocondaVariantAnnotationWorkaround(input.path(), unzippedInputVcf)));
        bash.addCommand(new RscriptFilter(unzippedInputVcf, outputFilteredVcf, outputFullVcf));
        bash.addCommand(new MvCommand(outputFullVcf + ".bgz", outputFullVcf + ".gz"));
        bash.addCommand(new MvCommand(outputFullVcf + ".bgz.tbi", outputFullVcf + ".gz.tbi"));
        bash.addCommand(new MvCommand(outputFilteredVcf + ".bgz", outputFilteredVcf + ".gz"));
        bash.addCommand(new MvCommand(outputFilteredVcf + ".bgz.tbi", outputFilteredVcf + ".gz.tbi"));
        bash.addCommand(() -> "cp " + input.path() + " " + output.path());
        bash.addCommand(() -> "cp " + input.path() + ".tbi " + output.path() + ".tbi");

        return bash;
    }
}

