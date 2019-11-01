package com.hartwig.pipeline.calling.structural.gridss.command;

import java.util.List;

import com.google.common.collect.ImmutableList;

public class SortVcf extends GridssCommand {
    private final String withBealn;
    private final String annotatedBealn;
    private final String outputPath;

    public SortVcf(final String withBealn, final String annotatedBealn, final String outputPath) {
        this.withBealn = withBealn;
        this.annotatedBealn = annotatedBealn;
        this.outputPath = outputPath;
    }

    @Override
    int memoryGb() {
        return 32;
    }

    @Override
    public String className() {
        return "picard.vcf.SortVcf";
    }

    @Override
    public List<GridssArgument> arguments() {
        return ImmutableList.of(new GridssArgument("i", withBealn),
                new GridssArgument("i", annotatedBealn),
                new GridssArgument("o", outputPath));
    }
}
