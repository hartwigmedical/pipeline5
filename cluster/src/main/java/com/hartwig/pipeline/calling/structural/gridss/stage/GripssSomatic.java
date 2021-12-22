package com.hartwig.pipeline.calling.structural.gridss.stage;

import java.util.Collections;
import java.util.List;

import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.SubStage;

public class GripssSomatic extends SubStage {

    public static final String NAMESPACE = "gripss";

    public static final String GRIPSS_SOMATIC_FILTERED = "gripss.filtered.somatic";
    public static final String GRIPSS_SOMATIC_UNFILTERED = "gripss.somatic";

    private final ResourceFiles resourceFiles;
    private final String gridssPath;
    private final String tumorSample;
    private final String referenceSample;

    public GripssSomatic(final ResourceFiles resourceFiles, String tumorSample, String referenceSample, final String gridssPath) {
        super(GRIPSS_SOMATIC_FILTERED, FileTypes.GZIPPED_VCF);
        this.resourceFiles = resourceFiles;
        this.tumorSample = tumorSample;
        this.referenceSample = referenceSample;
        this.gridssPath = gridssPath;
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        return Collections.singletonList(new GripssCommand(resourceFiles, tumorSample, referenceSample, gridssPath));
    }

    public String unfilteredVcf(final String sampleId)
    {
        return String.format("%s.%s.%s", sampleId, GRIPSS_SOMATIC_UNFILTERED, FileTypes.GZIPPED_VCF);
    }

}