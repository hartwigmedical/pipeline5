package com.hartwig.pipeline.calling.somatic;

import java.util.Collections;
import java.util.List;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;

class StrelkaPostProcess extends SubStage {

    private final String tumorSampleName;
    private final String bed;
    private final String recalibratedTumorBamPath;

    StrelkaPostProcess(final String tumorSampleName, final String bed, final String recalibratedTumorBamPath) {
        super("strelka.post.processed", OutputFile.GZIPPED_VCF);
        this.tumorSampleName = tumorSampleName;
        this.bed = bed;
        this.recalibratedTumorBamPath = recalibratedTumorBamPath;
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        return Collections.singletonList(new StrelkaPostProcessCommand(input.path(),
                output.path(),
                bed,
                tumorSampleName,
                recalibratedTumorBamPath));
    }
}
