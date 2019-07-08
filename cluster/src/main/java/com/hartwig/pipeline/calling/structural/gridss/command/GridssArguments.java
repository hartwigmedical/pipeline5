package com.hartwig.pipeline.calling.structural.gridss.command;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.hartwig.pipeline.calling.structural.gridss.GridssCommon;
import com.hartwig.pipeline.execution.vm.BashCommand;

public class GridssArguments implements BashCommand {
    private final List<GridssArgument> arguments;

    GridssArguments() {
        arguments = new ArrayList<>();
    }

    public GridssArguments add(final String key, final String value) {
        arguments.add(new GridssArgument(key, value));
        return this;
    }

    @Override
    public String asBash() {
        return arguments.stream().map(BashCommand::asBash).collect(Collectors.joining(" "));
    }

    GridssArguments addTempDir() {
        add("tmp_dir", "/tmp");
        return this;
    }

    GridssArguments addBlacklist() {
        add("blacklist", GridssCommon.blacklist());
        return this;
    }

    GridssArguments addConfigFile() {
        add("configuration_file", GridssCommon.configFile());
        return this;
    }
}
