package hmf.pipeline.gatk;

import static java.lang.String.format;

import static hmf.io.PipelineOutput.ALIGNED;
import static hmf.io.PipelineOutput.SORTED;

import java.io.IOException;

import hmf.io.OutputFile;
import hmf.io.PipelineOutput;
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
                new String[] { format("I=%s", OutputFile.of(ALIGNED, lane).path()), format("O=%s", OutputFile.of(SORTED, lane).path()),
                        "SORT_ORDER=coordinate" }).execute();
    }
}
