package com.hartwig.pipeline.calling.structural.gridss.stage;

import static java.lang.String.format;
import static java.util.Arrays.asList;

import static com.hartwig.pipeline.execution.vm.VmDirectories.outputFile;

import java.io.File;
import java.util.List;

import com.hartwig.pipeline.calling.structural.gridss.command.AssembleBreakends;
import com.hartwig.pipeline.calling.structural.gridss.command.CollectGridssMetrics;
import com.hartwig.pipeline.calling.structural.gridss.command.SoftClipsToSplitReads;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.unix.MkDirCommand;

import org.immutables.value.Value;

public class Assemble {
    private final CommandFactory factory;

    @Value.Immutable
    public interface AssembleResult {
        String assemblyBam();

        String svMetrics();

        List<BashCommand> commands();

        String workingDir();
    }

    public Assemble(final CommandFactory factory) {
        this.factory = factory;
    }

    public AssembleResult initialise(final String sampleBam, final String tumorBam, final String referenceGenome, final String jointName,
            final String configFile, final String blacklist) {
        AssembleBreakends assembler = factory.buildAssembleBreakends(sampleBam, tumorBam, referenceGenome, jointName, configFile, blacklist);
        String gridssWorkingDirForAssembleBam = outputFile(format("%s.gridss.working", new File(assembler.assemblyBam()).getName()));
        CollectGridssMetrics metrics = factory.buildCollectGridssMetrics(assembler.assemblyBam(), gridssWorkingDirForAssembleBam);

        String assembleSvOutputBam = format("%s/%s.sv.bam", gridssWorkingDirForAssembleBam, new File(assembler.assemblyBam()).getName());
        SoftClipsToSplitReads.ForAssemble clipsToReads =
                factory.buildSoftClipsToSplitReadsForAssemble(assembler.assemblyBam(), referenceGenome, assembleSvOutputBam);

        return ImmutableAssembleResult.builder()
                .commands(asList(new MkDirCommand(gridssWorkingDirForAssembleBam), assembler, metrics, clipsToReads))
                .assemblyBam(assembler.assemblyBam())
                .svMetrics(metrics.outputBaseFilename())
                .workingDir(gridssWorkingDirForAssembleBam)
                .build();
    }
}
