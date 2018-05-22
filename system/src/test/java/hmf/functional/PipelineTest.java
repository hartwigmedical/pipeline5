package hmf.functional;

import static hmf.testsupport.BamAssertions.assertThatOutput;
import static hmf.testsupport.TestSamples.CONFIGURATION;
import static hmf.testsupport.TestSamples.LANE_1;
import static hmf.testsupport.TestSamples.LANE_2;

import java.io.IOException;

import org.apache.spark.api.java.JavaSparkContext;
import org.bdgenomics.adam.rdd.ADAMContext;
import org.junit.BeforeClass;
import org.junit.Test;

import hmf.pipeline.Pipeline;
import hmf.pipeline.PipelineOutput;
import hmf.pipeline.adam.ADAMPipelines;
import hmf.pipeline.gatk.GATK4Pipelines;
import hmf.sample.RawSequencingOutput;
import hmf.sample.Reference;

public class PipelineTest {

    private static JavaSparkContext context;

    @BeforeClass
    public static void beforeClass() {
        context = new JavaSparkContext("local[1]", "pipelineTest");
    }

    @Test
    public void gatkBwaProducesEquivalentBAMToCurrentPipeline() throws Exception {
        producesEquivalentBAMToCurrentPipeline(GATK4Pipelines.sortedAligned(Reference.from(CONFIGURATION), context));
    }

    @Test
    public void adamBwaProducesEquivalentBAMToCurrentPipeline() throws Exception {
        producesEquivalentBAMToCurrentPipeline(ADAMPipelines.sortedAligned(Reference.from(CONFIGURATION), new ADAMContext(context.sc())));
    }

    private void producesEquivalentBAMToCurrentPipeline(final Pipeline victim) throws IOException {
        victim.execute(RawSequencingOutput.from(CONFIGURATION));
        assertThatOutput(LANE_1, PipelineOutput.SORTED).isEqualToExpected();
        assertThatOutput(LANE_2, PipelineOutput.SORTED).isEqualToExpected();
    }
}