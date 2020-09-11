package com.hartwig.pipeline.calling.germline;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.GatkCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.stages.SubStage;

public class SelectVariants extends SubStage {

    private final List<String> selectTypes;
    private final String referenceFasta;

    SelectVariants(final String variantType, final List<String> selectTypes, final String referenceFasta) {
        super("raw_" + variantType, FileTypes.VCF);
        this.selectTypes = selectTypes;
        this.referenceFasta = referenceFasta;
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        List<String> arguments = selectTypes.stream().flatMap(type -> Stream.of("-selectType", type)).collect(Collectors.toList());
        arguments.add("-R");
        arguments.add(referenceFasta);
        arguments.add("-V");
        arguments.add(input.path());
        arguments.add("-o");
        arguments.add(output.path());
        return Collections.singletonList(new GatkCommand(GermlineCaller.TOOL_HEAP,
                "SelectVariants",
                arguments.toArray(new String[0])));
    }
}
