package com.hartwig.functional;

import static com.hartwig.testsupport.Assertions.assertThatOutput;
import static com.hartwig.testsupport.TestConfigurations.HUNDREDK_READS_HISEQ;
import static com.hartwig.testsupport.TestConfigurations.HUNDREDK_READS_HISEQ_PATIENT_NAME;

import java.io.File;

import com.hartwig.io.OutputFile;
import com.hartwig.io.OutputType;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.adam.ADAMPipelines;
import com.hartwig.pipeline.adam.CoverageThreshold;
import com.hartwig.pipeline.runtime.patient.PatientReader;
import com.hartwig.testsupport.SparkContextSingleton;

import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaSparkContext;
import org.bdgenomics.adam.rdd.ADAMContext;
import org.junit.BeforeClass;
import org.junit.Test;

public class PipelineFunctionalTest {

    private static final Sample REFERENCE_SAMPLE =
            Sample.builder(HUNDREDK_READS_HISEQ.patient().directory(), HUNDREDK_READS_HISEQ_PATIENT_NAME + "R").build();
    private static final Sample TUMOUR_SAMPLE =
            Sample.builder(HUNDREDK_READS_HISEQ.patient().directory(), HUNDREDK_READS_HISEQ_PATIENT_NAME + "T").build();
    private static JavaSparkContext context;

    @BeforeClass
    public static void beforeClass() throws Exception {
        FileUtils.deleteDirectory(new File(OutputFile.RESULTS_DIRECTORY));
        context = SparkContextSingleton.instance();
    }

    @Test
    public void adamBamCreationMatchesCurrentPipelineOuput() throws Exception {
        ADAMPipelines.bamCreation(HUNDREDK_READS_HISEQ.referenceGenome().path(), HUNDREDK_READS_HISEQ.knownIndel().paths(),
                new ADAMContext(context.sc()),
                1, CoverageThreshold.of(1, 1e-12)).execute(PatientReader.from(HUNDREDK_READS_HISEQ));
        assertThatOutput(REFERENCE_SAMPLE, OutputType.DUPLICATE_MARKED).sorted().aligned().duplicatesMarked().isEqualToExpected();
        assertThatOutput(TUMOUR_SAMPLE, OutputType.DUPLICATE_MARKED).sorted().aligned().duplicatesMarked().isEqualToExpected();
    }
}