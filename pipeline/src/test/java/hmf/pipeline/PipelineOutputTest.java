package hmf.pipeline;

import static java.lang.String.format;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import hmf.sample.FlowCell;
import hmf.sample.Lane;
import hmf.sample.Sample;

public class PipelineOutputTest {

    private static final Sample SAMPLE = Sample.of("", "TEST_SAMPLE");

    @Test
    public void pathFollowsConventionForLane() {
        PipelineOutput victim = PipelineOutput.UNMAPPED;
        assertThat(victim.path(Lane.of(SAMPLE, 1))).isEqualTo(format("%s/results/TEST_SAMPLE_L001_unmapped.bam",
                System.getProperty("user.dir")));
    }

    @Test
    public void pathFollowsConventionForFlowCell() {
        PipelineOutput victim = PipelineOutput.UNMAPPED;
        assertThat(victim.path(FlowCell.builder().sample(SAMPLE).build())).isEqualTo(format("%s/results/TEST_SAMPLE_unmapped.bam",
                System.getProperty("user.dir")));
    }
}