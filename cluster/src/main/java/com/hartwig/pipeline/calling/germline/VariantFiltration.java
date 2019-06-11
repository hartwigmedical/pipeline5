package com.hartwig.pipeline.calling.germline;

import static java.lang.String.format;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.GatkCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;

public class VariantFiltration extends SubStage {

    private final Map<String, String> filterExpressions;
    private final String referenceFasta;

    VariantFiltration(final String variantType, Map<String, String> filterExpressions, final String referenceFasta) {
        super("filtered_" + variantType, OutputFile.VCF);
        this.filterExpressions = filterExpressions;
        this.referenceFasta = referenceFasta;
    }

    @Override
    public BashStartupScript bash(final OutputFile input, final OutputFile output, final BashStartupScript bash) {
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
        return bash.addCommand(new GatkCommand(GermlineCaller.TOOL_HEAP,
                "VariantFiltration",
                arguments.toArray(new String[arguments.size()])));
    }

    private static String wrapInQuotes(final String string) {
        return format("\"%s\"", string);
    }
}
