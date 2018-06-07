package hmf.functional;

import static hmf.testsupport.Assertions.assertThatOutput;
import static hmf.testsupport.TestSamples.HUNDREDK_READS_HISEQ;
import static hmf.testsupport.TestSamples.HUNDREDK_READS_HISEQ_FLOW_CELL;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.bdgenomics.adam.rdd.ADAMContext;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import hmf.pipeline.adam.ADAMPipelines;
import hmf.pipeline.gatk.GATK4Pipelines;
import hmf.sample.RawSequencingOutput;
import hmf.sample.Reference;

public class PipelineTest {

    private static JavaSparkContext context;

    @BeforeClass
    public static void beforeClass() {
        SparkConf conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").setMaster("local[8]")
                .setAppName("test");
        context = new JavaSparkContext(conf);
    }

    @Ignore("ADAM Preprocessor fails currently on this sample (finds far less duplicates than expected). More investigation necessary")
    @Test
    public void adamPreprocessingMatchesCurrentPipelineOuput() throws Exception {
        ADAMPipelines.preProcessing(Reference.from(HUNDREDK_READS_HISEQ), new ADAMContext(context.sc())).execute(RawSequencingOutput.from(HUNDREDK_READS_HISEQ));
        assertThatOutput(HUNDREDK_READS_HISEQ_FLOW_CELL).isEqualToExpected();
    }

    @Ignore("GATK preprocessor fails currently on this sample (duplicate key exception). More investigation necessary")
    @Test
    public void gatkPreprocessingMatchesCurrentPipelineOuput() throws Exception {
        GATK4Pipelines.preProcessing(Reference.from(HUNDREDK_READS_HISEQ), context).execute(RawSequencingOutput.from(HUNDREDK_READS_HISEQ));
        assertThatOutput(HUNDREDK_READS_HISEQ_FLOW_CELL).isEqualToExpected();
    }
}