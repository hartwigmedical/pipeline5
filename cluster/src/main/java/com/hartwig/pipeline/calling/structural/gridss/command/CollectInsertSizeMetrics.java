package com.hartwig.pipeline.calling.structural.gridss.command;

import static com.hartwig.pipeline.calling.structural.gridss.command.GridssArgument.NULL_FILE_EXTENSION;

import java.util.List;

import com.google.common.collect.ImmutableList;

public class CollectInsertSizeMetrics extends GridssCommand {
    private final String inputBam;
    private final String outputFullPathPrefix;

    public CollectInsertSizeMetrics(final String inputBam, final String outputFullPathPrefix) {
        this.inputBam = inputBam;
        this.outputFullPathPrefix = outputFullPathPrefix;
    }

    @Override
    public String className() {
        return "gridss.analysis.CollectGridssMetrics";
    }

    @Override
    public List<GridssArgument> arguments() {
        return ImmutableList.of(GridssArgument.ASSUME_SORTED,
                new GridssArgument("i", inputBam),
                new GridssArgument("o", outputFullPathPrefix),
                new GridssArgument("threshold_coverage", "50000"),
                NULL_FILE_EXTENSION,
                new GridssArgument("gridss_program", "null"),
                new GridssArgument("program", "null"),
                new GridssArgument("program", "CollectInsertSizeMetrics"),
                new GridssArgument("stop_after", "1000000"));
    }
}
