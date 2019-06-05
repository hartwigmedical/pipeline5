package com.hartwig.pipeline.calling.structural.gridss.stage;

import com.hartwig.pipeline.calling.structural.gridss.command.AssembleBreakends;
import com.hartwig.pipeline.calling.structural.gridss.command.CollectGridssMetrics;
import com.hartwig.pipeline.calling.structural.gridss.command.GridssToBashCommandConverter;
import com.hartwig.pipeline.calling.structural.gridss.command.SoftClipsToSplitReads;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.MkDirCommand;
import org.immutables.value.Value;

import java.io.File;
import java.util.List;

import static com.hartwig.pipeline.execution.vm.VmDirectories.outputFile;
import static java.lang.String.format;
import static java.util.Arrays.asList;

public class Assemble {
    private CommandFactory factory;
    private GridssToBashCommandConverter converter;

    @Value.Immutable
    public interface AssembleResult {
        String assemblyBam();
        String svMetrics();
        List<BashCommand> commands();
    }

    public Assemble(CommandFactory factory, GridssToBashCommandConverter converter) {
        this.factory = factory;
        this.converter = converter;
    }

    public AssembleResult initialise(String sampleBam, String tumorBam, String referenceGenome) {
        AssembleBreakends assembler = factory.buildAssembleBreakends(sampleBam, tumorBam, referenceGenome);
        CollectGridssMetrics metrics = factory.buildCollectGridssMetrics(assembler.assemblyBam());

        String gridssWorkingDirForAssembleBam = outputFile(format("%s.gridss.working", new File(assembler.assemblyBam()).getName()));
        String assembleSvOutputBam = format("%s/%s.sv.bam", gridssWorkingDirForAssembleBam, new File(assembler.assemblyBam()).getName());
        SoftClipsToSplitReads.ForAssemble clipsToReads = factory.buildSoftClipsToSplitReadsForAssemble(assembler.assemblyBam(), referenceGenome, assembleSvOutputBam);

        return ImmutableAssembleResult.builder().commands(
                asList(
                    new MkDirCommand(gridssWorkingDirForAssembleBam),
                    converter.convert(assembler),
                    converter.convert(metrics),
                    converter.convert(clipsToReads)))
                .assemblyBam(assembleSvOutputBam)
                .svMetrics(metrics.metrics())
                .build();
    }
}
