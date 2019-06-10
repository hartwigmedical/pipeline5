package com.hartwig.pipeline.calling.germline;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.GatkCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;

public class SelectVariants extends SubStage {

    private final List<String> selectTypes;
    private final String referenceFasta;

    SelectVariants(final String variantType, final List<String> selectTypes, final String referenceFasta) {
        super("raw_"+variantType, OutputFile.VCF);
        this.selectTypes = selectTypes;
        this.referenceFasta = referenceFasta;
    }

    @Override
    public BashStartupScript bash(final OutputFile input, final OutputFile output, final BashStartupScript bash) {
        List<String> arguments = new ArrayList<>();
        arguments.addAll(selectTypes.stream().flatMap(type -> Stream.of("-selectType", type)).collect(Collectors.toList()));
        arguments.add("-R");
        arguments.add(referenceFasta);
        arguments.add("-V");
        arguments.add(input.path());
        arguments.add("-o");
        arguments.add(output.path());
        return bash.addCommand(new GatkCommand(GermlineCaller.TOOL_HEAP, "SelectVariants", arguments.toArray(new String[arguments.size()])));
    }
}
