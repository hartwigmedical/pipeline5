package com.hartwig.pipeline.adam;

import com.hartwig.io.InputOutput;
import com.hartwig.pipeline.QCResult;
import com.hartwig.pipeline.QualityControl;

import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD;

public class ADAMReadCountCheck implements QualityControl<AlignmentRecordRDD> {

    private final long previousReadCount;

    ADAMReadCountCheck(final long previousReadCount) {
        this.previousReadCount = previousReadCount;
    }

    @Override
    public QCResult check(final InputOutput<AlignmentRecordRDD> toQC) {
        long readCount = countReads(toQC.payload());
        return readCount == previousReadCount
                ? QCResult.ok()
                : QCResult.failure(String.format("Read count check failed. Aligned BAM count of [%s] is different "
                                + "than current [%s] for output [%s] for sample [%s]",
                        previousReadCount,
                        readCount,
                        toQC.type(),
                        toQC.sample().name()));
    }

    static QualityControl<AlignmentRecordRDD> from(AlignmentRecordRDD initialAlignments) {
        return new ADAMReadCountCheck(countReads(initialAlignments));
    }

    private static long countReads(final AlignmentRecordRDD initialAlignments) {
        return initialAlignments.rdd().toJavaRDD().count();
    }
}
