package com.hartwig.pipeline.calling.somatic;

import com.hartwig.pipeline.execution.vm.BashStartupScript;

class SageHotspotsAnnotation extends SubStage {

    private final String knownHotspots;
    private final String sageHotspotVcf;

    SageHotspotsAnnotation(final String knownHotspots, final String sageHotspotVcf) {
        super("sage.hotspots.annotated", OutputFile.GZIPPED_VCF);
        this.knownHotspots = knownHotspots;
        this.sageHotspotVcf = sageHotspotVcf;
    }

    @Override
    BashStartupScript bash(final OutputFile input, final OutputFile output, final BashStartupScript bash) {
        return bash.addCommand(new SageAnnotationCommand(input.path(), sageHotspotVcf, knownHotspots, output.path()));
    }
}
