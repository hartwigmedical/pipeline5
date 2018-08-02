package com.hartwig.pipeline.adam;

import com.hartwig.io.InputOutput;
import com.hartwig.pipeline.QCResult;
import com.hartwig.pipeline.QualityControl;

import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ADAMReadCountCheck implements QualityControl<AlignmentRecordRDD> {

    private final static Logger LOGGER = LoggerFactory.getLogger(ADAMReadCountCheck.class);
    private final long previousReadCount;

    ADAMReadCountCheck(final long previousReadCount) {
        this.previousReadCount = previousReadCount;
    }

    @Override
    public QCResult check(final InputOutput<AlignmentRecordRDD> toQC) {
        LOGGER.info("Starting read count check for sample [{}] input to stage [{}]", toQC.sample().name(), toQC.type());
        long readCount = countReads(toQC.payload());
        QCResult result = readCount == previousReadCount
                ? QCResult.ok()
                : QCResult.failure(String.format("Read count check failed. Aligned BAM count of [%s] is different "
                                + "than current [%s] for output [%s] for sample [%s]",
                        previousReadCount,
                        readCount,
                        toQC.type(),
                        toQC.sample().name()));
        LOGGER.info("Completed read count check for sample [{}] input to stage [{}]. Count was [{}]", toQC.sample().name(),
                toQC.type(),
                readCount);
        return result;
    }

    static QualityControl<AlignmentRecordRDD> from(AlignmentRecordRDD initialAlignments) {
        return new ADAMReadCountCheck(countReads(initialAlignments));
    }

    private static long countReads(final AlignmentRecordRDD initialAlignments) {
        LOGGER.info("Starting initialization of read count from BWA output.");
        long count = initialAlignments.rdd().toJavaRDD().count();
        LOGGER.info("Completed initialization of read count from BWA output.");
        return count;
    }
}
