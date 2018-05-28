package hmf.io;

import static java.lang.String.format;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import hmf.sample.FlowCell;
import hmf.sample.Lane;
import hmf.sample.Sample;

public class OutputFileTest {

    private static final Sample SAMPLE = Sample.of("", "TEST_SAMPLE");

    @Test
    public void pathFollowsConventionForLane() {
        assertThat(OutputFile.of(PipelineOutput.UNMAPPED, Lane.of(SAMPLE, 1)).path()).isEqualTo(format(
                "%s/results/TEST_SAMPLE_L001_unmapped.bam",
                System.getProperty("user.dir")));
    }

    @Test
    public void pathFollowsConventionForFlowCell() {
        assertThat(OutputFile.of(PipelineOutput.UNMAPPED, FlowCell.builder().sample(SAMPLE).build()).path()).isEqualTo(format(
                "%s/results/TEST_SAMPLE_unmapped.bam",
                System.getProperty("user.dir")));
    }
}