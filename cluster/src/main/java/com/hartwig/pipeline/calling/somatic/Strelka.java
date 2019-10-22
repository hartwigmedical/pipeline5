package com.hartwig.pipeline.calling.somatic;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.execution.vm.VmDirectories;

class Strelka extends SubStage {

    private static final String STRELKA_ANALYSIS_DIRECTORY = "/strelkaAnalysis";
    private final String referenceBamPath;
    private final String tumorBamPath;
    private final String strelkaConfigPath;
    private final String referenceGenomePath;

    Strelka(final String referenceBamPath, final String tumorBamPath, final String strelkaConfigPath, final String referenceGenomePath) {
        super("strelka", "vcf");
        this.referenceBamPath = referenceBamPath;
        this.tumorBamPath = tumorBamPath;
        this.strelkaConfigPath = strelkaConfigPath;
        this.referenceGenomePath = referenceGenomePath;
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        String strelkaAnalysisOutput = VmDirectories.OUTPUT + STRELKA_ANALYSIS_DIRECTORY;
        return ImmutableList.of(new ConfigureStrelkaWorkflowCommand(tumorBamPath,
                        referenceBamPath,
                        strelkaConfigPath,
                        referenceGenomePath,
                        strelkaAnalysisOutput),
                new MakeStrelka(strelkaAnalysisOutput),
                new CombineVcfsCommand(referenceGenomePath,
                        strelkaAnalysisOutput + "/results/passed.somatic.snvs.vcf",
                        strelkaAnalysisOutput + "/results/passed.somatic.indels.vcf",
                        output.path()));
    }
}
