package com.hartwig.pipeline.calling.somatic;

import com.hartwig.pipeline.execution.vm.Bash;

class SageV2ApplicationCommand extends SageCommand {
    SageV2ApplicationCommand(final String tumorSampleName, final String tumorBamPath, final String referenceSampleName,
            final String referenceBamPath, final String hotspotsVcf, final String panelBed, final String highConfidenceBed,
            final String referenceGenomePath, final String outputVcf) {
        super("com.hartwig.hmftools.sage.SageApplication",
                "110G",
                "-tumor",
                tumorSampleName,
                "-tumor_bam",
                tumorBamPath,
                "-reference",
                referenceSampleName,
                "-reference_bam",
                referenceBamPath,
                "-hotspots",
                hotspotsVcf,
                "-panel_bed",
                panelBed,
                "-high_confidence_bed",
                highConfidenceBed,
                "-ref_genome",
                referenceGenomePath,
                "-out",
                outputVcf,
                "-threads",
                Bash.allCpus());
    }
}
