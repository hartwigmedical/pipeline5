package hmf.pipeline;

import static java.lang.String.format;

import static hmf.testsupport.BamAssertions.assertThatOutput;

import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;

public class PipelineTest {

    private static final String SAMPLE_NAME = "CPCT12345678R_HJJLGCCXX_S1_L001";

    @Test
    public void gatkBwaProducesEquivalentBAMToCurrentPipeline() throws Exception {
        JavaSparkContext context = new JavaSparkContext("local[1]", "pipelineTest");
        Pipeline victim = new Pipeline(context,
                Configuration.builder()
                        .sampleDirectory(System.getProperty("user.dir") + "/src/test/resources/samples")
                        .sampleName(SAMPLE_NAME)
                        .referencePath(format("%s/reference_genome/reference.fa", System.getProperty("user.home")))
                        .build());
        victim.execute();
        assertThatOutput(SAMPLE_NAME, PipelineOutput.ALIGNED).isEqualToExpected();
    }
}