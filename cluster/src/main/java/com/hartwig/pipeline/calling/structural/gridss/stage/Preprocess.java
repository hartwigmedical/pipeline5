package com.hartwig.pipeline.calling.structural.gridss.stage;

import static java.lang.String.format;

import static com.hartwig.pipeline.calling.structural.gridss.command.SambambaGridssSortCommand.sortByName;

import java.io.File;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.structural.gridss.command.CollectGridssMetricsAndExtractSvReads;
import com.hartwig.pipeline.calling.structural.gridss.command.CollectInsertSizeMetrics;
import com.hartwig.pipeline.calling.structural.gridss.command.ComputeSamTags;
import com.hartwig.pipeline.calling.structural.gridss.command.SambambaGridssSortCommand;
import com.hartwig.pipeline.calling.structural.gridss.command.SoftClipsToSplitReads;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.execution.vm.unix.MkDirCommand;
import com.hartwig.pipeline.execution.vm.unix.PipeCommands;

public class Preprocess extends SubStage {
    private final String inputBam;
    private final String workingDir;
    private final String sampleName;
    private final String referenceGenomePath;

    public Preprocess(String inputBam, String workingDir, String sampleName, String referenceGenomePath) {
        super("preprocess", OutputFile.BAM);
        this.inputBam = inputBam;
        this.workingDir = workingDir;
        this.sampleName = sampleName;
        this.referenceGenomePath = referenceGenomePath;
    }

    @Override
    public List<BashCommand> bash(OutputFile input, OutputFile output) {
        String inputBamBasename = new File(inputBam).getName();
        String insertSizeMetrics = format("%s/%s.insert_size_metrics", workingDir, inputBamBasename);
        String outputFullPathPrefix = format("%s/%s", workingDir, inputBamBasename);

        CollectInsertSizeMetrics collectInsertSizeMetrics = new CollectInsertSizeMetrics(inputBam, outputFullPathPrefix);
        CollectGridssMetricsAndExtractSvReads collectGridssMetricsAndExtractSvReads =
                new CollectGridssMetricsAndExtractSvReads(inputBam, sampleName, insertSizeMetrics, outputFullPathPrefix, workingDir);

        ComputeSamTags computeSamTags =
                new ComputeSamTags(collectGridssMetricsAndExtractSvReads.resultantBam(), referenceGenomePath, sampleName);
        String outputSvBam = format("%s/%s.sv.bam", workingDir, new File(inputBam).getName());
        return ImmutableList.of(new MkDirCommand(workingDir),
                collectInsertSizeMetrics,
                new PipeCommands(collectGridssMetricsAndExtractSvReads, sortByName(collectGridssMetricsAndExtractSvReads.resultantBam())),
                new PipeCommands(computeSamTags, SambambaGridssSortCommand.sortByDefault(computeSamTags.resultantBam())),
                new SoftClipsToSplitReads.ForPreprocess(computeSamTags.resultantBam(), referenceGenomePath, outputSvBam));
    }
}
