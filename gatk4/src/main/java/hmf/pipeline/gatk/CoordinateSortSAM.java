package hmf.pipeline.gatk;

import static java.lang.String.format;

import static hmf.pipeline.PipelineOutput.ALIGNED;
import static hmf.pipeline.PipelineOutput.SORTED;

import java.io.IOException;

import hmf.pipeline.Configuration;
import hmf.pipeline.PipelineOutput;
import hmf.pipeline.Stage;
import picard.sam.SortSam;

public class CoordinateSortSAM implements Stage {

    private final Configuration configuration;

    CoordinateSortSAM(final Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    public PipelineOutput output() {
        return PipelineOutput.SORTED;
    }

    @Override
    public void execute() throws IOException {
        PicardExecutor.of(new SortSam(),
                new String[] { format("I=%s", ALIGNED.path(configuration.sampleName())),
                        format("O=%s", SORTED.path(configuration.sampleName())), "SORT_ORDER=coordinate" }).execute();
    }
}
