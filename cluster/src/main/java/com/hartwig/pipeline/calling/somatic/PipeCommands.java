package com.hartwig.pipeline.calling.somatic;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.hartwig.pipeline.execution.vm.BashCommand;

public class PipeCommands implements BashCommand {

    private final List<BashCommand> pipedCommands;

    PipeCommands(final BashCommand... pipedCommands) {
        this.pipedCommands = Arrays.asList(pipedCommands);
    }

    @Override
    public String asBash() {
        return pipedCommands.stream().map(BashCommand::asBash).collect(Collectors.joining(" | "));
    }
}
