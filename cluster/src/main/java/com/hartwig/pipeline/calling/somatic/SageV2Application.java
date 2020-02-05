package com.hartwig.pipeline.calling.somatic;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.command.TabixCommand;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;

class SageV2Application extends SubStage {

    private final String hotspotsVcf;
    private final String panelBed;
    private final String highConfidenceBed;
    private final String referenceGenomePath;
    private final String tumorBamPath;
    private final String referenceBamPath;
    private final String tumorSampleName;
    private final String referenceSampleName;

    SageV2Application(final String hotspotsVcf, final String panelBed, final String highConfidenceBed, final String referenceGenomePath,
            final String tumorBamPath, final String referenceBamPath, final String tumorSampleName, final String referenceSampleName) {
        super("sage.variants", OutputFile.GZIPPED_VCF);
        this.hotspotsVcf = hotspotsVcf;
        this.panelBed = panelBed;
        this.highConfidenceBed = highConfidenceBed;
        this.referenceGenomePath = referenceGenomePath;
        this.tumorBamPath = tumorBamPath;
        this.referenceBamPath = referenceBamPath;
        this.tumorSampleName = tumorSampleName;
        this.referenceSampleName = referenceSampleName;
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        return ImmutableList.of(new SageV2ApplicationCommand(tumorSampleName,
                tumorBamPath,
                referenceSampleName,
                referenceBamPath,
                hotspotsVcf,
                panelBed,
                highConfidenceBed,
                referenceGenomePath,
                output.path()), new TabixCommand(output.path()));
    }
}
