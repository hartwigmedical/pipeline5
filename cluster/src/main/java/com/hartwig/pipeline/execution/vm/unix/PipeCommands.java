package com.hartwig.pipeline.execution.vm.unix;

import com.hartwig.pipeline.execution.vm.BashCommand;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class PipeCommands implements BashCommand {

    private final List<BashCommand> pipedCommands;

    public PipeCommands(final BashCommand... pipedCommands) {
        this.pipedCommands = Arrays.asList(pipedCommands);
    }

    @Override
    public String asBash() {
        return pipedCommands.stream().map(BashCommand::asBash).collect(Collectors.joining(" | "));
    }
}
