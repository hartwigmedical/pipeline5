package com.hartwig.pipeline.calling.somatic;

import com.hartwig.pipeline.execution.vm.BashStartupScript;

class SageHotspotsApplication extends SubStage {

    private final String knownHotspots;
    private final String codingRegions;
    private final String referenceGenomePath;
    private final String recalibratedTumorBamPath;
    private final String recalibratedReferenceBamPath;
    private final String tumorSampleName;
    private final String referenceSampleName;

    SageHotspotsApplication(final String knownHotspots, final String codingRegions, final String referenceGenomePath,
            final String recalibratedTumorBamPath, final String recalibratedReferenceBamPath, final String tumorSampleName,
            final String referenceSampleName) {
        super("sage.hotspots", OutputFile.GZIPPED_VCF);
        this.knownHotspots = knownHotspots;
        this.codingRegions = codingRegions;
        this.referenceGenomePath = referenceGenomePath;
        this.recalibratedTumorBamPath = recalibratedTumorBamPath;
        this.recalibratedReferenceBamPath = recalibratedReferenceBamPath;
        this.tumorSampleName = tumorSampleName;
        this.referenceSampleName = referenceSampleName;
    }

    @Override
    BashStartupScript bash(final OutputFile input, final OutputFile output, final BashStartupScript bash) {
        return bash.addCommand(new SageApplicationCommand(tumorSampleName,
                recalibratedTumorBamPath,
                referenceSampleName,
                recalibratedReferenceBamPath,
                knownHotspots,
                codingRegions,
                referenceGenomePath,
                output.path())).addCommand(new TabixCommand(output.path()));
    }
}
