package hmf.io;

import static java.lang.String.format;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import hmf.patient.Lane;
import hmf.patient.Sample;

public class OutputFileTest {

    private static final String SAMPLE_NAME = "TEST_SAMPLE";

    @Test
    public void pathFollowsConventionForLane() {
        assertThat(OutputFile.of(OutputType.UNMAPPED, Lane.of("", SAMPLE_NAME, 1)).path()).isEqualTo(format(
                "%s/results/TEST_SAMPLE_L001_unmapped.bam",
                System.getProperty("user.dir")));
    }

    @Test
    public void pathFollowsConventionForFlowCell() {
        assertThat(OutputFile.of(OutputType.UNMAPPED, Sample.builder("", SAMPLE_NAME).name(SAMPLE_NAME).build()).path()).isEqualTo(
                format(
                "%s/results/TEST_SAMPLE_unmapped.bam",
                System.getProperty("user.dir")));
    }
}