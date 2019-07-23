package com.hartwig.pipeline.calling.structural.gridss.stage;

import static java.lang.String.format;

import static com.hartwig.pipeline.execution.vm.VmDirectories.outputFile;

import java.io.File;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.structural.gridss.command.AssembleBreakends;
import com.hartwig.pipeline.calling.structural.gridss.command.CollectGridssMetrics;
import com.hartwig.pipeline.calling.structural.gridss.command.SoftClipsToSplitReads;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.execution.vm.unix.MkDirCommand;

public class Assemble extends SubStage {
    private final String sampleBam;
    private final String tumorBam;
    private final String referenceGenome;
    private final String jointName;
    private final String configFile;
    private final String blacklist;

    public Assemble(final String sampleBam, final String tumorBam, final String jointName, final String referenceGenome,
            final String configFile, final String blacklist) {
        super("assemble", OutputFile.BAM);
        this.sampleBam = sampleBam;
        this.tumorBam = tumorBam;
        this.referenceGenome = referenceGenome;
        this.jointName = jointName;
        this.configFile = configFile;
        this.blacklist = blacklist;
    }

    @Override
    public BashStartupScript bash(OutputFile input, OutputFile output, BashStartupScript bash) {
        String assemblyBam = completedBam();
        String assemblyBamBasename = new File(assemblyBam).getName();
        String workingDir = format("%s.gridss.working", assemblyBam);
        bash.addCommand(new MkDirCommand(workingDir));
        AssembleBreakends assembleBreakends = new AssembleBreakends(sampleBam, tumorBam, assemblyBam, referenceGenome, configFile, blacklist);
        bash.addCommand(assembleBreakends);
        bash.addCommand(new CollectGridssMetrics(assemblyBam, format("%s/%s", workingDir, assemblyBamBasename)));
        String assembleSvOutputBam = format("%s/%s.sv.bam", workingDir, assemblyBamBasename);
        bash.addCommand(new SoftClipsToSplitReads.ForAssemble(assemblyBam, referenceGenome, assembleSvOutputBam));
        return bash;
    }

    public String completedBam() {
        return VmDirectories.outputFile(jointName + ".assemble.bam");
    }
}
