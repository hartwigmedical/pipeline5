package com.hartwig.pipeline.execution.vm;

public class SubShellCommand implements BashCommand{

    private final BashCommand decorated;

    public SubShellCommand(final BashCommand decorated) {
        this.decorated = decorated;
    }

    @Override
    public String asBash() {
        return String.format("(%s)", decorated.asBash());
    }
}
