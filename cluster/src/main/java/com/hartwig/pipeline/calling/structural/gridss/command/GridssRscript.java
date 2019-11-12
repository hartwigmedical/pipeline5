package com.hartwig.pipeline.calling.structural.gridss.command;

import static java.lang.String.format;

import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.tools.Versions;

public abstract class GridssRscript implements BashCommand {
    static final String GRIDSS_RSCRIPT_DIR = format("%s/gridss/%s", VmDirectories.TOOLS, Versions.GRIDSS);

    abstract String scriptName();

    abstract String arguments();

    @Override
    public String asBash() {
        return format("Rscript %s/%s %s", GRIDSS_RSCRIPT_DIR, scriptName(), arguments());
    }
}
