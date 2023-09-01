package com.hartwig.pipeline.calling.sage;

import com.hartwig.pipeline.storage.OutputFile;
import com.hartwig.computeengine.execution.vm.command.BashCommand;
import com.hartwig.pipeline.calling.command.BcfToolsCommandListBuilder;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.stages.SubStage;

import java.util.List;

class SelectSamples extends SubStage {

    private final String[] samples;

    SelectSamples(final String... samples) {
        super("sage.sort", FileTypes.GZIPPED_VCF);
        this.samples = samples;
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        return new BcfToolsCommandListBuilder(input.path(), output.path()).withIndex().selectSample(samples).build();
    }
}
