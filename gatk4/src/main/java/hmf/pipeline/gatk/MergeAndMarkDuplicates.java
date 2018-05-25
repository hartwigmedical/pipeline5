package hmf.pipeline.gatk;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import hmf.pipeline.PipelineOutput;
import hmf.pipeline.Stage;
import hmf.sample.FlowCell;
import picard.sam.markduplicates.MarkDuplicates;

class MergeAndMarkDuplicates implements Stage<FlowCell> {

    @Override
    public PipelineOutput output() {
        return PipelineOutput.DEDUPED;
    }

    @Override
    public void execute(FlowCell flowCell) throws IOException {
        List<String> inputArgs =
                flowCell.lanes().stream().map(lane -> String.format("I=%s", PipelineOutput.SORTED.path(lane))).collect(Collectors.toList());
        inputArgs.add(String.format("O=%s", output().path(flowCell)));
        inputArgs.add(String.format("M=%s", "results/dupes.txt"));

        PicardExecutor.of(new MarkDuplicates(), inputArgs.toArray(new String[inputArgs.size()])).execute();
    }
}
