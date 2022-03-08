package com.hartwig.pipeline.calling.germline;

import static java.lang.String.format;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.GatkCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.stages.SubStage;

public class VariantFiltration extends SubStage {

    private final Map<String, String> filterExpressions;
    private final String referenceFasta;

    VariantFiltration(final String variantType, final Map<String, String> filterExpressions, final String referenceFasta) {
        super("filtered_" + variantType, FileTypes.VCF);
        this.filterExpressions = filterExpressions;
        this.referenceFasta = referenceFasta;
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        List<String> arguments = new ArrayList<>();
        arguments.add("-R");
        arguments.add(referenceFasta);
        arguments.add("-V");
        arguments.add(input.path());
        arguments.add("-o");
        arguments.add(output.path());
        arguments.addAll(filterExpressions.entrySet()
                .stream()
                .flatMap(entry -> Stream.of("--filterExpression",
                        wrapInQuotes(entry.getValue()),
                        "--filterName",
                        wrapInQuotes(entry.getKey())))
                .collect(Collectors.toList()));
        arguments.add("--clusterSize");
        arguments.add("3");
        arguments.add("--clusterWindowSize");
        arguments.add("35");
        return Collections.singletonList(new GatkCommand(GermlineCaller.TOOL_HEAP,
                "VariantFiltration",
                arguments.toArray(new String[0])));
    }

    private static String wrapInQuotes(final String string) {
        return format("\"%s\"", string);
    }
}
