package com.hartwig.pipeline.calling.germline;

import com.hartwig.pipeline.alignment.AlignmentOutput;

public class GermlineCaller {

    GermlineCaller() {
    }

    public GermlineCallerOutput run(AlignmentOutput alignmentOutput) {
        return GermlineCallerOutput.of("test", "wherever the vcf ends up");
    }
}
