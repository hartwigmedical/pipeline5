package com.hartwig.pipeline.calling.somatic;

import java.util.Collections;
import java.util.List;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;

class SageHotspotsAnnotation extends SubStage {

    private final String knownHotspots;
    private final String sageHotspotVcf;

    SageHotspotsAnnotation(final String knownHotspots, final String sageHotspotVcf) {
        super("sage.hotspots.annotated", OutputFile.GZIPPED_VCF);
        this.knownHotspots = knownHotspots;
        this.sageHotspotVcf = sageHotspotVcf;
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        return Collections.singletonList(new SageAnnotationCommand(input.path(), sageHotspotVcf, knownHotspots, output.path()));
    }
}
