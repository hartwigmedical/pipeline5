package com.hartwig.pipeline.calling.somatic;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.command.TabixCommand;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.execution.vm.unix.PipeCommands;

class SageFiltersAndAnnotations extends SubStage {

    private final String tumorName;

    SageFiltersAndAnnotations(final String tumorName) {
        super("sage.hotspots.filtered", OutputFile.GZIPPED_VCF);
        this.tumorName = tumorName;
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        return ImmutableList.of(new PipeCommands(new BcfToolsPipeableIncludeFilterCommand("'FILTER=\"PASS\"'", input.path()),
                new BcfToolsPipeableAnnotationCommand("INFO/HOTSPOT"),
                new BcfToolsPipeableAnnotationCommand("FILTER/LOW_CONFIDENCE"),
                new BcfToolsPipeableAnnotationCommand("FILTER/GERMLINE_INDEL"),
                new BcfToolsPipeableViewCommand(tumorName, output.path())), new TabixCommand(output.path()));
    }
}
