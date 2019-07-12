package com.hartwig.pipeline.calling.structural.gridss.command;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.OutputFile;

public class CollectGridssMetricsSubStage extends SubStage {
    private final String inputBam;
    private final String workingDirectory;

    public CollectGridssMetricsSubStage(final String inputBam, final String workingDirectory) {
        super("gridss.collectmetrics", "bam");
        this.inputBam = inputBam;
        this.workingDirectory = workingDirectory;
    }

    @Override
    public BashStartupScript bash(final OutputFile input, final OutputFile output, final BashStartupScript bash) {
        bash.addCommand(new CollectGridssMetrics(inputBam, workingDirectory));
        return bash;
    }
}
