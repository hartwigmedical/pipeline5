package com.hartwig.pipeline.calling.structural.gridss.stage;

import java.util.List;

import com.google.api.client.util.Lists;
import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.command.BgzipCommand;
import com.hartwig.pipeline.calling.command.TabixCommand;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;

public class GridssPassAndPonFilter extends SubStage {

    public GridssPassAndPonFilter() {
        super("gridss.somatic.filtered", OutputFile.GZIPPED_VCF);
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        List<BashCommand> result = Lists.newArrayList();
        final BgzipCommand bgzipCommand = new BgzipCommand();

        result.add(() -> String.format("gunzip -c %s | awk '$7 == \"PASS\" || $7 == \"PON\" || $1 ~ /^#/ ' | %s | tee %s > /dev/null",
                input.path(),
                bgzipCommand.asBash(),
                output.path()));
        result.add(new TabixCommand(output.path()));
        return result;
    }
}
