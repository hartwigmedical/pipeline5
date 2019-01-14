package com.hartwig.pipeline.adam;

import static com.hartwig.testsupport.TestRDDs.AlignmentRecordRDD;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.io.InputOutput;
import com.hartwig.io.OutputType;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.QCResult;
import com.hartwig.pipeline.QualityControl;

import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD;
import org.junit.Test;

public class ReadCountCheckTest {

    @Test
    public void countMatchesReturnsOk() {
        QCResult qcResult = readCountIs(100573);
        assertThat(qcResult.isOk()).as(qcResult.message()).isTrue();
    }

    @Test
    public void countDoesntMatchReturnsFailure() {
        QCResult qcResult = readCountIs(123);
        assertThat(qcResult.isOk()).isFalse();
    }

    private static QCResult readCountIs(final int previousReadCount) {
        AlignmentRecordRDD first = AlignmentRecordRDD("expected/TESTXR.bam");
        QualityControl<AlignmentRecordRDD> victim = new ReadCountCheck(previousReadCount);
        return victim.check(InputOutput.of(OutputType.DUPLICATE_MARKED, Sample.builder("", "test").build(), first));
    }
}