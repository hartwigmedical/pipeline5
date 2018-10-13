package com.hartwig.functional;

import static com.hartwig.testsupport.Assertions.assertThatOutput;
import static com.hartwig.testsupport.TestConfigurations.HUNDREDK_READS_HISEQ;
import static com.hartwig.testsupport.TestConfigurations.HUNDREDK_READS_HISEQ_PATIENT_NAME;

import java.io.File;

import com.hartwig.patient.Sample;
import com.hartwig.patient.input.PatientReader;
import com.hartwig.pipeline.adam.Pipelines;
import com.hartwig.pipeline.metrics.Monitor;
import com.hartwig.support.hadoop.Hadoop;
import com.hartwig.testsupport.SparkContextSingleton;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.api.java.JavaSparkContext;
import org.bdgenomics.adam.rdd.ADAMContext;
import org.junit.BeforeClass;
import org.junit.Test;

public class PipelineFunctionalTest {

    private static final Sample REFERENCE_SAMPLE =
            Sample.builder(HUNDREDK_READS_HISEQ.patient().directory(), HUNDREDK_READS_HISEQ_PATIENT_NAME + "R").build();
    private static JavaSparkContext context;

    private static String RESULT_DIR = System.getProperty("user.dir") + "/results/";

    @BeforeClass
    public static void beforeClass() throws Exception {
        FileUtils.deleteDirectory(new File(RESULT_DIR));
        context = SparkContextSingleton.instance();
    }

    @Test
    public void adamBamCreationMatchesCurrentPipelineOuput() throws Exception {
        FileSystem fileSystem = Hadoop.localFilesystem();
        Pipelines.bamCreationConsolidated(new ADAMContext(context.sc()),
                fileSystem,
                Monitor.noop(),
                RESULT_DIR,
                HUNDREDK_READS_HISEQ.referenceGenome().path(),
                HUNDREDK_READS_HISEQ.knownIndel().paths(),
                1,
                false, false)
                .execute(PatientReader.fromHDFS(fileSystem, HUNDREDK_READS_HISEQ.patient().directory(), HUNDREDK_READS_HISEQ_PATIENT_NAME)
                        .reference());
        assertThatOutput(RESULT_DIR, REFERENCE_SAMPLE).sorted().aligned().duplicatesMarked().isEqualToExpected();
    }
}