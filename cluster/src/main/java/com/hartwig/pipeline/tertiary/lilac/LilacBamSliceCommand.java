package com.hartwig.pipeline.tertiary.lilac;

import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.SambambaCommand;
import com.hartwig.pipeline.resource.ResourceFiles;

public class LilacBamSliceCommand implements BashCommand {
    private final ResourceFiles resourceFiles;
    private final String inputBam;
    private final String outputBam;

    LilacBamSliceCommand(ResourceFiles resourceFiles, String inputBam, String outputBam) {
        this.resourceFiles = resourceFiles;
        this.inputBam = inputBam;
        this.outputBam = outputBam;
    }

    @Override
    public String asBash() {
        return new SambambaCommand("slice", "-L", resourceFiles.hlaRegionBed(), "-o", outputBam, inputBam).asBash();
    }
}
