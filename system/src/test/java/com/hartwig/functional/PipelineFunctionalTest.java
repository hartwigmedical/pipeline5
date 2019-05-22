package com.hartwig.functional;

import static com.hartwig.testsupport.Assertions.assertThatOutput;
import static com.hartwig.testsupport.TestConfigurations.HUNDREDK_READS_HISEQ;
import static com.hartwig.testsupport.TestConfigurations.HUNDREDK_READS_HISEQ_PATIENT_NAME;

import com.hartwig.patient.Sample;
import com.hartwig.patient.input.PatientReader;
import com.hartwig.pipeline.adam.Pipelines;
import com.hartwig.pipeline.after.Processes;
import com.hartwig.pipeline.runtime.spark.SparkContexts;
import com.hartwig.support.hadoop.Hadoop;

import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.api.java.JavaSparkContext;
import org.bdgenomics.adam.rdd.ADAMContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipelineFunctionalTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(PipelineFunctionalTest.class);

    private static final Sample REFERENCE_SAMPLE =
            Sample.builder(HUNDREDK_READS_HISEQ.patient().directory(), HUNDREDK_READS_HISEQ_PATIENT_NAME + "R").build();
    private static final JavaSparkContext SPARK_CONTEXT = SparkContexts.create("function-test", HUNDREDK_READS_HISEQ);

    private static final String RESULT_DIR = System.getProperty("user.dir") + "/results/";

    @BeforeClass
    public static void beforeClass() throws Exception {
        LOGGER.info("Clearing out bwa shared memory before running test");
        Processes.run(new ProcessBuilder("bwa", "shm", "-d"));
    }

    @AfterClass
    public static void afterClass() {
        SPARK_CONTEXT.stop();
    }

    @Test
    public void adamBamCreationMatchesCurrentPipelineOutput() throws Exception {
        FileSystem fileSystem = Hadoop.localFilesystem();
        Pipelines.bamCreationConsolidated(new ADAMContext(SPARK_CONTEXT.sc()),
                fileSystem,
                RESULT_DIR,
                HUNDREDK_READS_HISEQ.referenceGenome().path(),
                1,
                true)
                .execute(PatientReader.fromHDFS(fileSystem, HUNDREDK_READS_HISEQ.patient().directory(), HUNDREDK_READS_HISEQ_PATIENT_NAME)
                        .reference());
        assertThatOutput(RESULT_DIR, REFERENCE_SAMPLE).aligned().duplicatesMarked().isEqualToExpected();
    }
}