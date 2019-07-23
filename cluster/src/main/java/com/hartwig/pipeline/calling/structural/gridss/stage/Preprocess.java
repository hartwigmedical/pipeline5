package com.hartwig.pipeline.calling.structural.gridss.stage;

import static java.lang.String.format;

import java.io.File;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.structural.gridss.command.CollectGridssMetrics;
import com.hartwig.pipeline.calling.structural.gridss.command.ComputeSamTags;
import com.hartwig.pipeline.calling.structural.gridss.command.ExtractSvReads;
import com.hartwig.pipeline.calling.structural.gridss.command.SambambaGridssSortCommand;
import com.hartwig.pipeline.calling.structural.gridss.command.SoftClipsToSplitReads;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.execution.vm.unix.PipeCommands;

public class Preprocess extends SubStage {
    private final String inputBam;
    private final String workingDir;
    private final String sampleName;
    private final String referenceGenomePath;
    private final String outputSvBam;

    public Preprocess(String inputBam, String workingDir, String sampleName, String referenceGenomePath, String outputSvBam) {
        super("preprocess", OutputFile.BAM);
        this.inputBam = inputBam;
        this.workingDir = workingDir;
        this.sampleName = sampleName;
        this.referenceGenomePath = referenceGenomePath;
        this.outputSvBam = outputSvBam;
    }

    @Override
    public BashStartupScript bash(OutputFile input, OutputFile output, BashStartupScript bash) {
        String inputBamBasename = new File(inputBam).getName();
        CollectGridssMetrics collectGridssMetrics = new CollectGridssMetrics(inputBam, format("%s/%s", workingDir, inputBamBasename));
        bash.addCommand(collectGridssMetrics);
        String insertSizeMetrics = format("%s/%s.insert_size_metrics", workingDir, inputBamBasename);
        ExtractSvReads extractSvReads = new ExtractSvReads(inputBam, sampleName, insertSizeMetrics, workingDir);
        bash.addCommand(new PipeCommands(extractSvReads, SambambaGridssSortCommand.sortByName(extractSvReads.resultantBam())));
        ComputeSamTags computeSamTags = new ComputeSamTags(extractSvReads.resultantBam(), referenceGenomePath, sampleName);
        bash.addCommand(new PipeCommands(computeSamTags, SambambaGridssSortCommand.sortByDefault(computeSamTags.resultantBam())));
        bash.addCommand(new SoftClipsToSplitReads.ForPreprocess(computeSamTags.resultantBam(), referenceGenomePath, outputSvBam));
        return bash;
    }
}
