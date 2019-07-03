package com.hartwig.pipeline.calling.structural.gridss.stage;

import static java.lang.String.format;
import static java.util.Arrays.asList;

import static com.hartwig.pipeline.execution.vm.VmDirectories.outputFile;

import java.io.File;
import java.util.List;

import com.hartwig.pipeline.calling.structural.gridss.command.AssembleBreakends;
import com.hartwig.pipeline.calling.structural.gridss.command.CollectGridssMetrics;
import com.hartwig.pipeline.calling.structural.gridss.command.GridssToBashCommandConverter;
import com.hartwig.pipeline.calling.structural.gridss.command.SoftClipsToSplitReads;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.unix.MkDirCommand;

import org.immutables.value.Value;

public class Assemble {
    private final CommandFactory factory;
    private final GridssToBashCommandConverter converter;

    @Value.Immutable
    public interface AssembleResult {
        String assemblyBam();

        String svMetrics();

        List<BashCommand> commands();

        String workingDir();
    }

    public Assemble(final CommandFactory factory, final GridssToBashCommandConverter converter) {
        this.factory = factory;
        this.converter = converter;
    }

    public AssembleResult initialise(final String sampleBam, final String tumorBam, final String referenceGenome, final String jointName) {
        AssembleBreakends assembler = factory.buildAssembleBreakends(sampleBam, tumorBam, referenceGenome, jointName);
        CollectGridssMetrics metrics = factory.buildCollectGridssMetrics(assembler.assemblyBam());

        String gridssWorkingDirForAssembleBam = outputFile(format("%s.gridss.working", new File(assembler.assemblyBam()).getName()));
        String assembleSvOutputBam = format("%s/%s.sv.bam", gridssWorkingDirForAssembleBam, new File(assembler.assemblyBam()).getName());
        SoftClipsToSplitReads.ForAssemble clipsToReads =
                factory.buildSoftClipsToSplitReadsForAssemble(assembler.assemblyBam(), referenceGenome, assembleSvOutputBam);

        return ImmutableAssembleResult.builder()
                .commands(asList(new MkDirCommand(gridssWorkingDirForAssembleBam),
                        converter.convert(assembler),
                        converter.convert(metrics),
                        converter.convert(clipsToReads)))
                .assemblyBam(assembleSvOutputBam)
                .svMetrics(metrics.outputBaseFilename())
                .workingDir(gridssWorkingDirForAssembleBam)
                .build();
    }
}
