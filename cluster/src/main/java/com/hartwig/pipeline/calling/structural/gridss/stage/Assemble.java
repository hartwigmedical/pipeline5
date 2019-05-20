package com.hartwig.pipeline.calling.structural.gridss.stage;

import com.hartwig.pipeline.calling.structural.gridss.process.AssembleBreakends;
import com.hartwig.pipeline.calling.structural.gridss.process.CollectGridssMetrics;
import com.hartwig.pipeline.calling.structural.gridss.process.SoftClipsToSplitReads;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import org.immutables.value.Value;

import java.io.File;
import java.util.List;

import static java.lang.String.format;
import static java.util.Arrays.asList;

public class Assemble {
    private CommandFactory factory;

    @Value.Immutable
    public interface AssembleResult {
        String assemblyBam();
        String svMetrics();
        List<BashCommand> commands();

    }

    public Assemble(CommandFactory factory) {
        this.factory = factory;
    }

    public AssembleResult initialise(String sampleBam, String tumorBam, String referenceGenome) {
        AssembleBreakends assembler = factory.buildAssembleBreakends(sampleBam, tumorBam, referenceGenome);
        CollectGridssMetrics metrics = factory.buildCollectGridssMetrics(assembler.assemblyBam());

        String gridssWorkingDirForAssembleBam = format("%s/%s.gridss.working", VmDirectories.OUTPUT, new File(assembler.assemblyBam()).getName());
        BashCommand dirMaker = () -> "mkdir -p " + gridssWorkingDirForAssembleBam;

        String assembleSvOutputBam = format("%s/%s.sv.bam", gridssWorkingDirForAssembleBam, new File(assembler.assemblyBam()).getName());
        SoftClipsToSplitReads.ForAssemble clipsToReads = factory.buildSoftClipsToSplitReadsForAssemble(assembler.assemblyBam(), referenceGenome, assembleSvOutputBam);

        return ImmutableAssembleResult.builder()
                .commands(asList(dirMaker, assembler, metrics, clipsToReads))
                .assemblyBam(assembleSvOutputBam)
                .svMetrics(metrics.metrics())
                .build();
    }
}
