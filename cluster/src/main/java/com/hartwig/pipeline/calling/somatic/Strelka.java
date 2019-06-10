package com.hartwig.pipeline.calling.somatic;

import static com.hartwig.pipeline.execution.vm.OutputUpload.OUTPUT_DIRECTORY;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.OutputFile;

class Strelka extends SubStage {

    private static final String STRELKA_ANALYSIS_DIRECTORY = "/strelkaAnalysis";
    private final String recalibratedReferenceBamPath;
    private final String recalibratedTumorBamPath;
    private final String strelkaConfigPath;
    private final String referenceGenomePath;

    Strelka(final String recalibratedReferenceBamPath, final String recalibratedTumorBamPath, final String strelkaConfigPath,
            final String referenceGenomePath) {
        super("strelka", "vcf");
        this.recalibratedReferenceBamPath = recalibratedReferenceBamPath;
        this.recalibratedTumorBamPath = recalibratedTumorBamPath;
        this.strelkaConfigPath = strelkaConfigPath;
        this.referenceGenomePath = referenceGenomePath;
    }

    @Override
    public BashStartupScript bash(final OutputFile input, final OutputFile output, final BashStartupScript bash) {
        String strelkaAnalysisOutput = OUTPUT_DIRECTORY + STRELKA_ANALYSIS_DIRECTORY;
        return bash.addCommand(new ConfigureStrelkaWorkflowCommand(recalibratedTumorBamPath,
                recalibratedReferenceBamPath,
                strelkaConfigPath,
                referenceGenomePath,
                strelkaAnalysisOutput))
                .addCommand(new MakeStrelka(strelkaAnalysisOutput))
                .addCommand(new CombineVcfsCommand(referenceGenomePath,
                        strelkaAnalysisOutput + "/results/passed.somatic.snvs.vcf",
                        strelkaAnalysisOutput + "/results/passed.somatic.indels.vcf",
                        output.path()));
    }
}
