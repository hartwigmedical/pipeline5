package com.hartwig.pipeline.calling.structural.gridss.stage;

import java.util.Collections;
import java.util.List;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.structural.gridss.command.GripssSoftFilterCommand;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.resource.ResourceFiles;

public class GridssSomaticFilter extends SubStage {

    private final ResourceFiles resourceFiles;

    public GridssSomaticFilter(final ResourceFiles resourceFiles) {
        super("gridss.somatic", OutputFile.GZIPPED_VCF);
        this.resourceFiles = resourceFiles;
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        return Collections.singletonList(new GripssSoftFilterCommand(resourceFiles, input.path(), output.path()));
    }
}
