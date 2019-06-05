package com.hartwig.pipeline.calling.structural.gridss.command;

import com.hartwig.pipeline.calling.structural.gridss.GridssCommon;
import com.hartwig.pipeline.execution.vm.BashCommand;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class GridssArguments implements BashCommand {
    private final List<GridssArgument> arguments;

    GridssArguments() {
        arguments = new ArrayList<>();
    }

    public GridssArguments add(String key, String value) {
        arguments.add(new GridssArgument(key, value));
        return this;
    }

    @Override
    public String asBash() {
        return arguments.stream().map(BashCommand::asBash).collect(Collectors.joining(" "));
    }

    public GridssArguments addTempDir() {
        add("tmp_dir", "/tmp");
        return this;
    }

    public GridssArguments addBlacklist() {
        add("blacklist", GridssCommon.blacklist());
        return this;
    }

    public GridssArguments addConfigFile() {
        add("configuration_file", GridssCommon.configFile());
        return this;
    }
}
