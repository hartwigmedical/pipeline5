package com.hartwig.pipeline.calling.somatic;

import java.util.List;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.command.BcfToolsCommandListBuilder;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;

class SageFiltersAndAnnotations extends SubStage {

    private final String tumorName;

    SageFiltersAndAnnotations(final String tumorName) {
        super("sage.hotspots.filtered", OutputFile.GZIPPED_VCF);
        this.tumorName = tumorName;
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        return new BcfToolsCommandListBuilder(input.path(), output.path()).withIndex()
                .includeHardPass()
                .removeAnnotation("INFO/HOTSPOT")
                .removeAnnotation("FILTER/LOW_CONFIDENCE")
                .removeAnnotation("FILTER/GERMLINE_INDEL")
                .selectSample(tumorName)
                .build();
    }
}
