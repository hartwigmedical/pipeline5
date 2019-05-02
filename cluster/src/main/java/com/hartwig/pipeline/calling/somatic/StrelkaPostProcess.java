package com.hartwig.pipeline.calling.somatic;

import com.hartwig.pipeline.execution.vm.BashStartupScript;
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
    BashStartupScript bash(final OutputFile input, final OutputFile output, final BashStartupScript bash) {
        return bash.addCommand(new StrelkaPostProcessCommand(input.path(), output.path(), bed, tumorSampleName, recalibratedTumorBamPath));
    }
}
