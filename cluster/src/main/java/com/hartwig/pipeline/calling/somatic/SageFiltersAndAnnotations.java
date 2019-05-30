package com.hartwig.pipeline.calling.somatic;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.command.TabixCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.OutputFile;

class SageFiltersAndAnnotations extends SubStage {

    private final String tumorName;

    SageFiltersAndAnnotations(final String tumorName) {
        super("sage.hotspots.filtered", OutputFile.GZIPPED_VCF);
        this.tumorName = tumorName;
    }

    @Override
    public BashStartupScript bash(final OutputFile input, final OutputFile output, final BashStartupScript bash) {
        return bash.addCommand(new PipeCommands(new BcfToolsPipeableIncludeFilterCommand("'FILTER=\"PASS\"'", input.path()),
                new BcfToolsPipeableAnnotationCommand("INFO/HOTSPOT"),
                new BcfToolsPipeableAnnotationCommand("FILTER/LOW_CONFIDENCE"),
                new BcfToolsPipeableAnnotationCommand("FILTER/GERMLINE_INDEL"),
                new BcfToolsPipeableViewCommand(tumorName, output.path()))).addCommand(new TabixCommand(output.path()));
    }
}
