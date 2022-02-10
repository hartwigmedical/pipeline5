package com.hartwig.pipeline.calling.structural.gripss;

import java.util.Collections;
import java.util.List;

import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.SubStage;

public class GripssGermline extends SubStage {

    public static final String NAMESPACE = "gripss";

    public static final String GRIPSS_GERMLINE_FILTERED = "gripss.filtered.germline";
    public static final String GRIPSS_GERMLINE_UNFILTERED = "gripss.germline";

    private final ResourceFiles resourceFiles;
    private final String gridssPath;
    private final String referenceSample;

    public GripssGermline(final ResourceFiles resourceFiles, String referenceSample, final String gridssPath) {
        super(GRIPSS_GERMLINE_FILTERED, FileTypes.GZIPPED_VCF);
        this.resourceFiles = resourceFiles;
        this.referenceSample = referenceSample;
        this.gridssPath = gridssPath;
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        return Collections.singletonList(new GripssCommand(resourceFiles, referenceSample, gridssPath));
    }

    public String unfilteredVcf(final String sampleId)
    {
        return String.format("%s.%s.%s", sampleId, GRIPSS_GERMLINE_UNFILTERED, FileTypes.GZIPPED_VCF);
    }

}