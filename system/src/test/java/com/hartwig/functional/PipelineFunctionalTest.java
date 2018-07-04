package com.hartwig.functional;

import static com.hartwig.testsupport.Assertions.assertThatOutput;
import static com.hartwig.testsupport.TestConfigurations.HUNDREDK_READS_HISEQ;
import static com.hartwig.testsupport.TestConfigurations.HUNDREDK_READS_HISEQ_PATIENT_NAME;

import java.io.File;

import com.hartwig.io.OutputFile;
import com.hartwig.io.OutputType;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.adam.ADAMPipelines;
import com.hartwig.pipeline.gatk.GATK4Pipelines;
import com.hartwig.pipeline.runtime.patient.PatientReader;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.bdgenomics.adam.rdd.ADAMContext;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class PipelineFunctionalTest {

    private static final Sample SAMPLE =
            Sample.builder(HUNDREDK_READS_HISEQ.patient().directory(), HUNDREDK_READS_HISEQ_PATIENT_NAME + "R").build();
    private static JavaSparkContext context;

    @BeforeClass
    public static void beforeClass() throws Exception {
        FileUtils.deleteDirectory(new File(OutputFile.RESULTS_DIRECTORY));
        SparkConf conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").setMaster("local[2]")
                .setAppName("test");
        context = new JavaSparkContext(conf);
    }

    @Test
    public void adamPreprocessingMatchesCurrentPipelineOuput() throws Exception {
        ADAMPipelines.preProcessing(HUNDREDK_READS_HISEQ.patient().referenceGenomePath(),
                HUNDREDK_READS_HISEQ.patient().knownIndelPaths(),
                new ADAMContext(context.sc()),
                1).execute(PatientReader.from(HUNDREDK_READS_HISEQ));
        assertThatOutput(SAMPLE, OutputType.DUPLICATE_MARKED).sorted().aligned().duplicatesMarked().isEqualToExpected();
    }

    @Ignore("GATK preprocessor fails currently on this sample (duplicate key exception). More investigation necessary")
    @Test
    public void gatkPreprocessingMatchesCurrentPipelineOuput() throws Exception {
        GATK4Pipelines.preProcessing(HUNDREDK_READS_HISEQ.patient().referenceGenomePath(), context)
                .execute(PatientReader.from(HUNDREDK_READS_HISEQ));
        assertThatOutput(SAMPLE, OutputType.DUPLICATE_MARKED).aligned().duplicatesMarked().isEqualToExpected();
    }
}