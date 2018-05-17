package hmf.functional;

import static hmf.testsupport.BamAssertions.assertThatOutput;

import java.io.IOException;

import org.apache.spark.api.java.JavaSparkContext;
import org.bdgenomics.adam.rdd.ADAMContext;
import org.junit.BeforeClass;
import org.junit.Test;

import hmf.pipeline.Configuration;
import hmf.pipeline.Pipeline;
import hmf.pipeline.PipelineOutput;
import hmf.pipeline.adam.ADAMPipelines;
import hmf.pipeline.gatk.GATK4Pipelines;

public class PipelineTest {

    private static final String SAMPLE_NAME = "CPCT12345678R_HJJLGCCXX_S1_L001_chr22";
    private static final Configuration CONFIGURATION = Configuration.builder()
            .sampleDirectory(System.getProperty("user.dir") + "/src/test/resources/samples")
            .sampleName(SAMPLE_NAME)
            .referencePath(System.getProperty("user.dir") + "/src/test/resources/reference/Homo_sapiens.GRCh37.GATK.illumina.chr22.fa")
            .build();
    private static JavaSparkContext context;

    @BeforeClass
    public static void beforeClass() {
        context = new JavaSparkContext("local[1]", "pipelineTest");
    }

    @Test
    public void gatkBwaProducesEquivalentBAMToCurrentPipeline() throws Exception {
        producesEquivalentBAMToCurrentPipeline(GATK4Pipelines.sortedAligned(CONFIGURATION, context));
    }

    @Test
    public void adamBwaProducesEquivalentBAMToCurrentPipeline() throws Exception {
        producesEquivalentBAMToCurrentPipeline(ADAMPipelines.sortedAligned(CONFIGURATION, new ADAMContext(context.sc())));
    }

    private void producesEquivalentBAMToCurrentPipeline(final Pipeline victim) throws IOException {
        victim.execute();
        assertThatOutput(SAMPLE_NAME, PipelineOutput.SORTED).isEqualToExpected();
    }
}