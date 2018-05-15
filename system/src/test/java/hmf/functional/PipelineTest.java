package hmf.functional;

import static hmf.testsupport.BamAssertions.assertThatOutput;

import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;

import hmf.pipeline.Configuration;
import hmf.pipeline.Pipeline;
import hmf.pipeline.PipelineOutput;
import hmf.pipeline.gatk.GATK4Pipelines;

public class PipelineTest {

    private static final String SAMPLE_NAME = "CPCT12345678R_HJJLGCCXX_S1_L001_chr22";

    @Test
    public void gatkBwaProducesEquivalentBAMToCurrentPipeline() throws Exception {
        JavaSparkContext context = new JavaSparkContext("local[1]", "pipelineTest");
        Configuration configuration = Configuration.builder()
                .sampleDirectory(System.getProperty("user.dir") + "/src/test/resources/samples")
                .sampleName(SAMPLE_NAME)
                .referencePath(System.getProperty("user.dir") + "/src/test/resources/reference/Homo_sapiens.GRCh37.GATK.illumina.chr22.fa")
                .build();
        Pipeline victim = GATK4Pipelines.sortedAligned(configuration, context);
        victim.execute();
        assertThatOutput(SAMPLE_NAME, PipelineOutput.SORTED).isEqualToExpected();
    }
}