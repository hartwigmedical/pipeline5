package hmf.functional;

import static hmf.testsupport.Assertions.assertThatOutput;
import static hmf.testsupport.TestSamples.CANCER_PANEL;
import static hmf.testsupport.TestSamples.CANCER_PANEL_LANE_1;
import static hmf.testsupport.TestSamples.CANCER_PANEL_LANE_2;
import static hmf.testsupport.TestSamples.HUNDREDK_READS_HISEQ;
import static hmf.testsupport.TestSamples.HUNDREDK_READS_HISEQ_FLOW_CELL;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.bdgenomics.adam.rdd.ADAMContext;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import hmf.pipeline.Configuration;
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
        SparkConf conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .setMaster("local[1]")
                .setAppName("test");
        context = new JavaSparkContext(conf);
    }

    @Test
    public void gatkBwaProducesEquivalentBAMToCurrentPipeline() throws Exception {
        producesEquivalentAlignedBAMToCurrentPipeline(GATK4Pipelines.sortedAligned(Reference.from(CANCER_PANEL), context), CANCER_PANEL);
    }

    @Test
    public void adamBwaProducesEquivalentBAMToCurrentPipeline() throws Exception {
        producesEquivalentAlignedBAMToCurrentPipeline(ADAMPipelines.sortedAligned(Reference.from(CANCER_PANEL),
                new ADAMContext(context.sc())), CANCER_PANEL);
    }

    @Ignore
    @Test
    public void gatkMergeMarkDupsProducesSingleBAMWithDupsMarked() throws Exception {
        GATK4Pipelines.sortedAlignedDupsMarked(Reference.from(HUNDREDK_READS_HISEQ), context)
                .execute(RawSequencingOutput.from(HUNDREDK_READS_HISEQ));
        assertThatOutput(HUNDREDK_READS_HISEQ_FLOW_CELL).isEqualToExpected();
    }

    private void producesEquivalentAlignedBAMToCurrentPipeline(final Pipeline victim, final Configuration configuration)
            throws IOException {
        victim.execute(RawSequencingOutput.from(configuration));
        assertThatOutput(CANCER_PANEL_LANE_1, PipelineOutput.SORTED).isEqualToExpected();
        assertThatOutput(CANCER_PANEL_LANE_2, PipelineOutput.SORTED).isEqualToExpected();
    }
}