package com.hartwig.pipeline.calling.substages;

import java.util.List;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.command.BcfToolsCommandListBuilder;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;

public class CosmicAnnotation extends SubStage {

    private final String cosmicDB;
    private final String columns;

    public CosmicAnnotation(final String cosmicDB, final String columns) {
        super("cosmic.annotated", OutputFile.GZIPPED_VCF);
        this.cosmicDB = cosmicDB;
        this.columns = columns;
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        return new BcfToolsCommandListBuilder(input.path(), output.path()).withIndex().addAnnotation(cosmicDB, columns).build();
    }
}