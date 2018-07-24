package com.hartwig.pipeline.adam;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.io.InputOutput;
import com.hartwig.io.OutputType;
import com.hartwig.patient.ReferenceGenome;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.QCResult;
import com.hartwig.pipeline.QualityControl;
import com.hartwig.testsupport.TestConfigurations;
import com.hartwig.testsupport.TestRDDs;

import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD;
import org.junit.Test;

public class ADAMFinalBAMQCTest {

    private static final AlignmentRecordRDD CANCER_PANEL_RDD = TestRDDs.alignmentRecordRDD("qc/CPCT12345678R_duplicate_marked.bam");

    @Test
    public void checkFailsOnEmptyInput() {
        QualityControl<AlignmentRecordRDD> victim = qc(CoverageThreshold.of(1, 1));
        QCResult test =
                victim.check(InputOutput.of(OutputType.MD_TAGGED, Sample.builder("", "test").build(), TestRDDs.emptyAlignnmentRecordRDD()));
        assertThat(test.isOk()).isFalse();
    }

    @Test
    public void checkPassesOnNoThresholds() {
        QualityControl<AlignmentRecordRDD> victim = qc();
        QCResult test = victim.check(InputOutput.of(OutputType.MD_TAGGED, Sample.builder("", "test").build(), CANCER_PANEL_RDD));
        assertThat(test.isOk()).isTrue();
    }

    @Test
    public void checkFailsOnThresholdMissed() {
        QualityControl<AlignmentRecordRDD> victim = qc(CoverageThreshold.of(10, 18));
        QCResult test = victim.check(InputOutput.of(OutputType.MD_TAGGED, Sample.builder("", "test").build(), CANCER_PANEL_RDD));
        assertThat(test.isOk()).isFalse();
    }

    @Test
    public void checkPassesOnThresholdMet() {
        QualityControl<AlignmentRecordRDD> victim = qc(CoverageThreshold.of(10, 16));
        QCResult test = victim.check(InputOutput.of(OutputType.MD_TAGGED, Sample.builder("", "test").build(), CANCER_PANEL_RDD));
        assertThat(test.isOk()).isTrue();
    }

    private ADAMFinalBAMQC qc(final CoverageThreshold... coverageThreshold) {
        return ADAMFinalBAMQC.of(TestRDDs.javaAdam(),
                ReferenceGenome.of(TestConfigurations.REFERENCE_GENOME_PARAMETERS.path()),
                coverageThreshold);
    }
}