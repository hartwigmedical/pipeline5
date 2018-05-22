package hmf.pipeline.gatk;

import static java.lang.String.format;

import static hmf.pipeline.PipelineOutput.ALIGNED;
import static hmf.pipeline.PipelineOutput.SORTED;

import java.io.IOException;

import hmf.pipeline.PipelineOutput;
import hmf.pipeline.Stage;
import hmf.sample.Lane;
import picard.sam.SortSam;

public class CoordinateSortSAM implements Stage<Lane> {

    @Override
    public PipelineOutput output() {
        return PipelineOutput.SORTED;
    }

    @Override
    public void execute(Lane lane) throws IOException {
        PicardExecutor.of(new SortSam(),
                new String[] { format("I=%s", ALIGNED.path(lane)), format("O=%s", SORTED.path(lane)), "SORT_ORDER=coordinate" }).execute();
    }
}
