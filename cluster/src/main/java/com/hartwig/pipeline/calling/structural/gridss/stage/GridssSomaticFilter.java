package com.hartwig.pipeline.calling.structural.gridss.stage;

import java.util.Collections;
import java.util.List;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.structural.gridss.command.GripssCommand;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.resource.ResourceFiles;

public class GridssSomaticFilter extends SubStage {

    public static final String GRIDSS_SOMATIC = "gridss.somatic";
    private final ResourceFiles resourceFiles;

    public GridssSomaticFilter(final ResourceFiles resourceFiles) {
        super(GRIDSS_SOMATIC, OutputFile.GZIPPED_VCF);
        this.resourceFiles = resourceFiles;
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        return Collections.singletonList(new GripssCommand(resourceFiles, input.path(), output.path()));
    }
}
