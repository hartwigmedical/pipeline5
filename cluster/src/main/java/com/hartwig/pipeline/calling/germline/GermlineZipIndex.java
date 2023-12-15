package com.hartwig.pipeline.calling.germline;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.computeengine.execution.vm.command.BashCommand;
import com.hartwig.pipeline.calling.command.BgzipCommand;
import com.hartwig.pipeline.calling.command.TabixCommand;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.execution.OutputFile;
import com.hartwig.pipeline.stages.SubStage;

public class GermlineZipIndex extends SubStage {

    public GermlineZipIndex() {
        super("filtered_variants", FileTypes.GZIPPED_VCF);
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        String beforeZip = output.path().replace(".gz", "");

        return ImmutableList.of(
                new BgzipCommand(beforeZip),
                new TabixCommand(output.path()));
    }
}
