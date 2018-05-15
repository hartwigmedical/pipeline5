package hmf.pipeline.gatk;

import static java.lang.String.format;

import hmf.pipeline.Configuration;
import hmf.pipeline.PipelineOutput;
import hmf.pipeline.Stage;
import picard.sam.FastqToSam;

public class UBAMFromFastQ implements Stage {

    private final Configuration configuration;

    UBAMFromFastQ(final Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    public PipelineOutput output() {
        return PipelineOutput.UNMAPPED;
    }

    @Override
    public void execute() {
        PicardExecutor.of(new FastqToSam(),
                new String[] { readFileArgumentOf(1, configuration), readFileArgumentOf(2, configuration),
                        "SM=" + configuration.sampleName(), "O=" + PipelineOutput.UNMAPPED.path(configuration.sampleName()) }).execute();
    }

    private static String readFileArgumentOf(int sampleIndex, Configuration configuration) {
        return format("F%s=%s/%s_R%s.fastq", sampleIndex, configuration.sampleDirectory(), configuration.sampleName(), sampleIndex);
    }
}
