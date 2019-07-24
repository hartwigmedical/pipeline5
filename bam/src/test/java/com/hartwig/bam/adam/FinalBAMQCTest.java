package com.hartwig.bam.adam;

import static com.hartwig.testsupport.TestConfigurations.HUNDREDK_READS_HISEQ;
import static com.hartwig.testsupport.TestConfigurations.REFERENCE_GENOME_PARAMETERS;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.bam.QCResult;
import com.hartwig.bam.QualityControl;
import com.hartwig.bam.runtime.spark.SparkContexts;
import com.hartwig.io.InputOutput;
import com.hartwig.patient.ReferenceGenome;
import com.hartwig.patient.Sample;
import com.hartwig.testsupport.TestRDDs;

import org.apache.spark.api.java.JavaSparkContext;
import org.bdgenomics.adam.rdd.read.AlignmentRecordDataset;
import org.junit.AfterClass;
import org.junit.Test;

public class FinalBAMQCTest {

    private static final JavaSparkContext SPARK_CONTEXT = SparkContexts.create("final-bamqc-test", HUNDREDK_READS_HISEQ);
    private static final AlignmentRecordDataset CANCER_PANEL_RDD = TestRDDs.alignmentRecordDataset("qc/CPCT12345678R.bam", SPARK_CONTEXT);

    @AfterClass
    public static void afterClass() {
        SPARK_CONTEXT.stop();
    }

    @Test
    public void checkFailsOnEmptyInput() {
        QualityControl<AlignmentRecordDataset> victim = qc(CoverageThreshold.of(1, 1));
        QCResult test = victim.check(InputOutput.of(Sample.builder("", "test").build(), TestRDDs.emptyAlignmentRecordRDD(SPARK_CONTEXT)));
        assertThat(test.isOk()).isFalse();
    }

    @Test
    public void checkPassesOnNoThresholds() {
        QualityControl<AlignmentRecordDataset> victim = qc();
        QCResult test = victim.check(InputOutput.of(Sample.builder("", "test").build(), CANCER_PANEL_RDD));
        assertThat(test.isOk()).isTrue();
    }

    @Test
    public void checkFailsOnThresholdMissed() {
        QualityControl<AlignmentRecordDataset> victim = qc(CoverageThreshold.of(5, 0.012));
        QCResult test = victim.check(InputOutput.of(Sample.builder("", "test").build(), CANCER_PANEL_RDD));
        assertThat(test.isOk()).as(test.message()).isFalse();
    }

    @Test
    public void checkPassesOnThresholdMet() {
        QualityControl<AlignmentRecordDataset> victim = qc(CoverageThreshold.of(5, 0.0108));
        QCResult test = victim.check(InputOutput.of(Sample.builder("", "test").build(), CANCER_PANEL_RDD));
        assertThat(test.isOk()).isTrue();
    }

    private FinalBAMQC qc(final CoverageThreshold... coverageThreshold) {
        return FinalBAMQC.of(TestRDDs.javaAdam(SPARK_CONTEXT), ReferenceGenome.of(REFERENCE_GENOME_PARAMETERS.path()),
                coverageThreshold);
    }
}