package com.hartwig.pipeline.calling.germline;

import com.hartwig.pipeline.alignment.AlignmentOutput;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.cluster.vm.GoogleVirtualMachine;

public class GermlineCaller {

    private final Arguments arguments;
    private final GoogleVirtualMachine germlineVM;

    public GermlineCaller(final Arguments arguments, final GoogleVirtualMachine germlineVM) {
        this.arguments = arguments;
        this.germlineVM = germlineVM;
    }

    public GermlineCallerOutput run(AlignmentOutput alignmentOutput) {
        germlineVM.run();
        return GermlineCallerOutput.of("test", "wherever the vcf ends up");
    }
}
