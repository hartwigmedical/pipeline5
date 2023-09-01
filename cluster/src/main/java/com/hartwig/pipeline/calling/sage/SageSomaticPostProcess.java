package com.hartwig.pipeline.calling.sage;

import com.google.api.client.util.Lists;
import com.hartwig.pipeline.storage.OutputFile;
import com.hartwig.computeengine.execution.vm.command.BashCommand;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.stages.SubStage;
import com.hartwig.pipeline.stages.SubStageInputOutput;

import java.util.List;

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
