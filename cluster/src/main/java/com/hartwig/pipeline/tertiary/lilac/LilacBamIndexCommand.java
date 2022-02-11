package com.hartwig.pipeline.tertiary.lilac;

import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.SambambaCommand;

public class LilacBamIndexCommand implements BashCommand {
    private final String inputBam;

    LilacBamIndexCommand(final String inputBam) {
        this.inputBam = inputBam;
    }

    @Override
    public String asBash() {
        return new SambambaCommand("index", inputBam).asBash();
    }
}
