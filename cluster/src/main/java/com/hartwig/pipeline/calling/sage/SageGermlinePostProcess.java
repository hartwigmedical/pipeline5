package com.hartwig.pipeline.calling.sage;

import com.google.api.client.util.Lists;
import com.hartwig.pipeline.storage.OutputFile;
import com.hartwig.computeengine.execution.vm.command.BashCommand;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.stages.SubStage;
import com.hartwig.pipeline.stages.SubStageInputOutput;

import java.util.List;

public class SageGermlinePostProcess extends SubStage {

    public static final String SAGE_GERMLINE_FILTERED = "sage.germline.filtered";
    private final SubStageInputOutput sampleName;

    public SageGermlinePostProcess(final String sampleName) {
        super(SAGE_GERMLINE_FILTERED, FileTypes.GZIPPED_VCF);
        this.sampleName = SubStageInputOutput.empty(sampleName);
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        final List<BashCommand> result = Lists.newArrayList();

        SubStage passFilter = new PassFilter();

        OutputFile finalOutputFile = OutputFile.of(sampleName.sampleName(), SAGE_GERMLINE_FILTERED, FileTypes.GZIPPED_VCF);
        result.addAll(passFilter.bash(input, finalOutputFile));
        return result;
    }
}
