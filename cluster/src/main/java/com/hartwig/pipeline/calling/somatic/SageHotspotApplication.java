package com.hartwig.pipeline.calling.somatic;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.command.TabixCommand;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;

class SageHotspotApplication extends SubStage {

    private final String knownHotspots;
    private final String codingRegions;
    private final String referenceGenomePath;
    private final String tumorBamPath;
    private final String referenceBamPath;
    private final String tumorSampleName;
    private final String referenceSampleName;

    SageHotspotApplication(final String knownHotspots, final String codingRegions, final String referenceGenomePath,
            final String tumorBamPath, final String referenceBamPath, final String tumorSampleName, final String referenceSampleName) {
        super("sage.hotspots", OutputFile.GZIPPED_VCF);
        this.knownHotspots = knownHotspots;
        this.codingRegions = codingRegions;
        this.referenceGenomePath = referenceGenomePath;
        this.tumorBamPath = tumorBamPath;
        this.referenceBamPath = referenceBamPath;
        this.tumorSampleName = tumorSampleName;
        this.referenceSampleName = referenceSampleName;
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        return ImmutableList.of(new SageHotspotApplicationCommand(tumorSampleName,
                tumorBamPath,
                referenceSampleName,
                referenceBamPath,
                knownHotspots,
                codingRegions,
                referenceGenomePath,
                output.path()), new TabixCommand(output.path()));
    }
}
