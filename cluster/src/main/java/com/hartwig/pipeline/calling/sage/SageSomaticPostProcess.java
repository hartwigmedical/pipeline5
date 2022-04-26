package com.hartwig.pipeline.calling.sage;

import java.util.List;

import com.google.api.client.util.Lists;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.stages.SubStage;
import com.hartwig.pipeline.stages.SubStageInputOutput;

public class SageSomaticPostProcess extends SubStage {

    public static final String SAGE_SOMATIC_FILTERED = "sage.somatic.filtered";
    private final SubStageInputOutput tumorSampleName;

    public SageSomaticPostProcess(final String tumorSampleName) {
        super(SAGE_SOMATIC_FILTERED, FileTypes.GZIPPED_VCF);
        this.tumorSampleName = SubStageInputOutput.empty(tumorSampleName);
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        final List<BashCommand> result = Lists.newArrayList();

        SubStage passFilter = new PassFilter();

        OutputFile finalOutputFile = OutputFile.of(tumorSampleName.sampleName(), SAGE_SOMATIC_FILTERED, FileTypes.GZIPPED_VCF);
        result.addAll(passFilter.bash(input, finalOutputFile));

        return result;
    }
}
