package hmf.pipeline;

import static java.lang.String.format;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class FilesystemOutputTest {

    @Test
    public void pathComposedOfWorkingDirectorySampleNameAndType() {
        PipelineOutput victim = PipelineOutput.UNMAPPED;
        assertThat(victim.path("TEST_SAMPLE")).isEqualTo(format("%s/results/TEST_SAMPLE_unmapped.bam", System.getProperty("user.dir")));
    }
}