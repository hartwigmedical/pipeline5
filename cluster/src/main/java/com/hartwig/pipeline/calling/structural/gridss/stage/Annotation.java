package com.hartwig.pipeline.calling.structural.gridss.stage;

import static java.lang.String.format;
import static java.util.Arrays.asList;

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
    private final String rawVcf;
    private final String referenceGenome;
    private final String jointName;
    private final String configFile;
    private final String blacklist;

    public Annotation(final String sampleBam, final String tumorBam, final String assemblyBam, final String rawVcf,
                      final String referenceGenome, final String jointName, final String configFile, final String blacklist) {
        super("annotation", OutputFile.GZIPPED_VCF);
        this.sampleBam = sampleBam;
        this.tumorBam = tumorBam;
        this.assemblyBam = assemblyBam;
        this.rawVcf = rawVcf;
        this.referenceGenome = referenceGenome;
        this.jointName = jointName;
        this.configFile = configFile;
        this.blacklist = blacklist;
    }

    @Override
    public BashStartupScript bash(OutputFile input, OutputFile output, BashStartupScript bash) {
        String annotatedVcf = VmDirectories.outputFile(jointName + ".annotated_variants.vcf");
        AnnotateVariants variants = new AnnotateVariants(sampleBam, tumorBam, assemblyBam, input.path(), referenceGenome, annotatedVcf,
                        configFile, blacklist);
        String untemplatedOutputVcf = VmDirectories.outputFile(jointName + "_untemplated.vcf");
        AnnotateUntemplatedSequence untemplated = new AnnotateUntemplatedSequence(annotatedVcf, referenceGenome, untemplatedOutputVcf);
        BgzipCommand bgzip = new BgzipCommand(untemplatedOutputVcf);
        String finalOutputPath = format("%s.gz", untemplatedOutputVcf);
        TabixCommand tabix = new TabixCommand(finalOutputPath);
        bash.addCommands(asList(variants, untemplated, bgzip, tabix));
        return bash;
    }
}
