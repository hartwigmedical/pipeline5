package com.hartwig.pipeline.execution.vm;

import java.util.List;
import java.util.stream.Collectors;

public class JavaCommand implements BashCommand {

    private final String jar;
    private final String maxHeapSize;
    private final List<String> arguments;

    JavaCommand(final String jar, final String maxHeapSize, final List<String> arguments) {
        this.jar = jar;
        this.maxHeapSize = maxHeapSize;
        this.arguments = arguments;
    }

    @Override
    public String asBash() {
        return String.format("java -Xmx%s -jar %s %s", maxHeapSize, jar, arguments.stream().collect(Collectors.joining(" ")));
    }
}
