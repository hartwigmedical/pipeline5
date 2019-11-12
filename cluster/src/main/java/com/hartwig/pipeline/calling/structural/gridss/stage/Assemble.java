package com.hartwig.pipeline.calling.structural.gridss.stage;

import static java.lang.String.format;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.structural.gridss.command.AssembleBreakends;
import com.hartwig.pipeline.calling.structural.gridss.command.CollectGridssMetrics;
import com.hartwig.pipeline.calling.structural.gridss.command.SoftClipsToSplitReads;
import com.hartwig.pipeline.execution.vm.BashCommand;
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
    public List<BashCommand> bash(OutputFile input, OutputFile output) {

        List<BashCommand> bash = new ArrayList<>();
        String assemblyBam = completedBam();
        String assemblyBamBasename = new File(assemblyBam).getName();
        String workingDir = format("%s.gridss.working", assemblyBam);
        bash.add(new MkDirCommand(workingDir));
        AssembleBreakends assembleBreakends =
                new AssembleBreakends(sampleBam, tumorBam, assemblyBam, referenceGenome, configFile, blacklist);
        bash.add(assembleBreakends);
        bash.add(new CollectGridssMetrics(assemblyBam, format("%s/%s", workingDir, assemblyBamBasename)));
        String assembleSvOutputBam = format("%s/%s.sv.bam", workingDir, assemblyBamBasename);
        bash.add(new SoftClipsToSplitReads.ForAssemble(assemblyBam, referenceGenome, assembleSvOutputBam));
        return bash;
    }

    public String completedBam() {
        return VmDirectories.outputFile(jointName + ".assemble.bam");
    }
}
