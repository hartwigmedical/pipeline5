package com.hartwig.pipeline.calling.structural.gridss.stage;

import java.util.Collections;
import java.util.List;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.structural.gridss.command.GripssSoftFilterCommand;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.resource.ResourceFiles;

public class GridssSomaticFilter extends SubStage {

    public static final String GRIDSS_SOMATIC = "gripss.somatic";
    private final ResourceFiles resourceFiles;
    private final String gridssPath;

    public GridssSomaticFilter(final ResourceFiles resourceFiles, final String gridssPath) {
        super(GRIDSS_SOMATIC, OutputFile.GZIPPED_VCF);
        this.resourceFiles = resourceFiles;
        this.gridssPath = gridssPath;
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        return Collections.singletonList(new GripssSoftFilterCommand(resourceFiles, gridssPath, output.path()));
    }
}
