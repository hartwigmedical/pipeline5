package hmf.pipeline.gatk;

import static java.lang.String.format;

import hmf.io.OutputFile;
import hmf.io.PipelineOutput;
import hmf.pipeline.LaneStage;
import hmf.sample.Lane;
import picard.sam.FastqToSam;

public class UBAMFromFastQ implements LaneStage {

    @Override
    public PipelineOutput output() {
        return PipelineOutput.UNMAPPED;
    }

    @Override
    public void execute(Lane lane) {
        PicardExecutor.of(new FastqToSam(),
                new String[] { readFileArgumentOf(1, lane), readFileArgumentOf(2, lane), "SM=" + lane.sample().name(),
                        "O=" + OutputFile.of(PipelineOutput.UNMAPPED, lane).path() }).execute();
    }

    private static String readFileArgumentOf(int sampleIndex, Lane lane) {
        return format("F%s=%s/%s_L00%s_R%s.fastq", sampleIndex, lane.sample().directory(), lane.sample().name(), lane.index(), sampleIndex);
    }
}
