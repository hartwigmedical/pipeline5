package hmf.pipeline;

import static java.lang.String.format;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import hmf.sample.Lane;
import hmf.sample.Sample;

public class PipelineOutputTest {

    @Test
    public void pathComposedOfWorkingDirectorySampleNameAndType() {
        PipelineOutput victim = PipelineOutput.UNMAPPED;
        assertThat(victim.path(Lane.of(Sample.of("", "TEST_SAMPLE"), 1))).isEqualTo(format("%s/results/TEST_SAMPLE_unmapped.bam",
                System.getProperty("user.dir")));
    }
}