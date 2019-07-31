package com.hartwig.pipeline.calling.structural.gridss.stage;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.command.BgzipCommand;
import com.hartwig.pipeline.calling.command.TabixCommand;
import com.hartwig.pipeline.calling.structural.gridss.command.AnnotateUntemplatedSequence;
import com.hartwig.pipeline.calling.structural.gridss.command.AnnotateVariants;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.execution.vm.VmDirectories;

public class Annotation extends SubStage {
    private final String sampleBam;
    private final String tumorBam;
    private final String assemblyBam;
    private final String referenceGenome;
    private final String jointName;
    private final String configFile;
    private final String blacklist;

    public Annotation(final String sampleBam, final String tumorBam, final String assemblyBam, final String referenceGenome,
            final String jointName, final String configFile, final String blacklist) {
        super("annotation", OutputFile.GZIPPED_VCF);
        this.sampleBam = sampleBam;
        this.tumorBam = tumorBam;
        this.assemblyBam = assemblyBam;
        this.referenceGenome = referenceGenome;
        this.jointName = jointName;
        this.configFile = configFile;
        this.blacklist = blacklist;
    }

    @Override
    public BashStartupScript bash(OutputFile input, OutputFile output, BashStartupScript bash) {
        String annotatedVcf = VmDirectories.outputFile(jointName + ".annotated_variants.vcf");
        String untemplatedOutputVcf = output.path().replaceAll("\\." + OutputFile.GZIPPED_VCF + "$", ".vcf");

        return bash.addCommand(new AnnotateVariants(sampleBam, tumorBam, assemblyBam, input.path(), referenceGenome, annotatedVcf,
                configFile, blacklist))
                .addCommand(new AnnotateUntemplatedSequence(annotatedVcf, referenceGenome, untemplatedOutputVcf))
                .addCommand(new BgzipCommand(untemplatedOutputVcf))
                .addCommand(new TabixCommand(output.path()));
    }
}
