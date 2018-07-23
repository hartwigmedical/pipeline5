package com.hartwig.pipeline.adam;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.io.InputOutput;
import com.hartwig.io.OutputType;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.QCResult;
import com.hartwig.pipeline.QualityControl;
import com.hartwig.testsupport.SparkContextSingleton;

import org.bdgenomics.adam.api.java.JavaADAMContext;
import org.bdgenomics.adam.rdd.ADAMContext;
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD;
import org.junit.Test;

public class ADAMReadCountCheckTest {

    @Test
    public void countMatchesRetrunsOk() {
        QCResult qcResult = readCountIs(15318);
        assertThat(qcResult.isOk()).as(qcResult.message()).isTrue();
    }

    @Test
    public void countDoesntMatchReturnsFailure() {
        QCResult qcResult = readCountIs(123);
        assertThat(qcResult.isOk()).isFalse();
    }

    private static QCResult readCountIs(final int previousReadCount) {
        AlignmentRecordRDD first = new JavaADAMContext(new ADAMContext(SparkContextSingleton.instance().sc())).loadAlignments(
                System.getProperty("user.dir") + "/src/test/resources/expected/TESTXR_duplicate_marked.bam");
        QualityControl<AlignmentRecordRDD> victim = new ADAMReadCountCheck(previousReadCount);
        return victim.check(InputOutput.of(OutputType.DUPLICATE_MARKED, Sample.builder("", "test").build(), first));
    }
}