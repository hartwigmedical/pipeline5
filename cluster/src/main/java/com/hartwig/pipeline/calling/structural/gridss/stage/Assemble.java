package com.hartwig.pipeline.calling.structural.gridss.stage;

import com.hartwig.pipeline.calling.structural.gridss.process.AssembleBreakends;
import com.hartwig.pipeline.calling.structural.gridss.process.CollectGridssMetrics;
import com.hartwig.pipeline.calling.structural.gridss.process.SoftClipsToSplitReads;
import com.hartwig.pipeline.execution.vm.BashCommand;
import org.immutables.value.Value;

import java.util.List;

import static java.util.Arrays.asList;

public class Assemble {
    private CommandFactory factory;

    @Value.Immutable
    public interface AssembleResult {
        String svBam();
        String svMetrics();
        List<BashCommand> commands();

    }

    public Assemble(CommandFactory factory) {
        this.factory = factory;
    }

    public AssembleResult initialise(String sampleBam, String tumorBam, String referenceGenome, String blacklist) {
        AssembleBreakends assembler = factory.buildAssembleBreakends(sampleBam, tumorBam, referenceGenome, blacklist);
        CollectGridssMetrics metrics = factory.buildCollectGridssMetrics(assembler.resultantBam());
        SoftClipsToSplitReads.ForAssemble clipsToReads = factory.buildSoftClipsToSplitReadsForAssemble(assembler.resultantBam(), referenceGenome);

        return ImmutableAssembleResult.builder()
                .commands(asList(assembler, metrics, clipsToReads))
                .svBam(clipsToReads.resultantBam())
                .svMetrics(metrics.metrics())
                .build();
    }
}
