package com.hartwig.pipeline.alignment.bwa;

import static com.hartwig.pipeline.tools.ExternalTool.BWA_MEM2;

import com.hartwig.pipeline.calling.command.VersionedToolCommand;

public class BwaMem2Command extends VersionedToolCommand
{
    public BwaMem2Command(final String... arguments) {
        super(BWA_MEM2.getToolName(), BWA_MEM2.getBinary(), BWA_MEM2.getVersion(), arguments);
    }
}