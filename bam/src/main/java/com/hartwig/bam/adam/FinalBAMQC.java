package com.hartwig.bam.adam;

import static java.lang.String.format;

import static htsjdk.samtools.SAMUtils.fastqToPhred;

import java.io.Serializable;
import java.util.Collection;
import java.util.stream.IntStream;

import com.google.common.collect.Lists;
import com.hartwig.bam.QCResult;
import com.hartwig.bam.QualityControl;
import com.hartwig.io.InputOutput;
import com.hartwig.patient.ReferenceGenome;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.bdgenomics.adam.api.java.JavaADAMContext;
import org.bdgenomics.adam.rdd.read.AlignmentRecordDataset;
import org.bdgenomics.formats.avro.AlignmentRecord;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

@SuppressWarnings({ "FieldCanBeLocal", "unused" })
public class FinalBAMQC implements QualityControl<AlignmentRecordDataset>, Serializable {

    private final static Logger LOGGER = LoggerFactory.getLogger(FinalBAMQC.class);
    private final Collection<CoverageThreshold> thresholds;
    private final JavaADAMContext adamContext;
    private final ReferenceGenome referenceGenome;

    private FinalBAMQC(final Collection<CoverageThreshold> thresholds, final JavaADAMContext adamContext,
            final ReferenceGenome referenceGenome) {
        this.thresholds = thresholds;
        this.adamContext = adamContext;
        this.referenceGenome = referenceGenome;
    }

    public static FinalBAMQC of(final JavaADAMContext adamContext, final ReferenceGenome referenceGenome,
            final CoverageThreshold... thresholds) {
        return new FinalBAMQC(Lists.newArrayList(thresholds), adamContext, referenceGenome);
    }

    @Override
    public QCResult check(final InputOutput<AlignmentRecordDataset> toQC) {
        if (toQC.payload().rdd().isEmpty()) {
            return QCResult.failure("Final QC failed as the BAM was empty");
        }

        long totalCalledBases = adamContext.ac()
                .loadFastaDna(referenceGenome.path())
                .rdd()
                .toJavaRDD()
                .mapToDouble(sequence -> calledBases(sequence.getSequence()).length())
                .sum()
                .longValue();

        JavaDoubleRDD coverage = filterReads(toQC).rdd()
                .toJavaRDD()
                .flatMapToPair(record -> IntStream.range(record.getStart().intValue(), record.getEnd().intValue())
                        .mapToObj(readPos -> Tuple2.apply(readPos, ReferencePositions.getReadPositionAtReferencePosition(record, readPos)))
                        .filter(positionPair -> positionPair._2 != 0)
                        .filter(positionPair -> fastqToPhred(record.getQuality().charAt(positionPair._2)) >= 10)
                        .map(positionPair -> Tuple2.apply(positionPair._1, 1))
                        .iterator())
                .reduceByKey((v1, v2) -> v1 + v2)
                .mapToDouble(v -> v._2.doubleValue())
                .cache();

        for (CoverageThreshold threshold : thresholds) {
            long totalExceeding = coverage.filter(count -> count > threshold.coverage()).count();
            double percentage = percentage(totalCalledBases, totalExceeding);
            LOGGER.info(format("BAM QC [%sx at %f%%]: Results [%s of %s at %dx coverage] or [%f%%]",
                    threshold.coverage(),
                    threshold.minimumPercentage(),
                    totalExceeding,
                    totalCalledBases,
                    threshold.coverage(),
                    percentage));
            if (percentage < threshold.minimumPercentage()) {
                return QCResult.failure(format(
                        "Final QC failed on sample [%s] as [%dx] " + "coverage was below the minimum percentage of [%f%%]",
                        toQC.sample().name(),
                        threshold.coverage(),
                        threshold.minimumPercentage()));
            }
        }

        return QCResult.ok();
    }

    @NotNull
    private static AlignmentRecordDataset filterReads(final InputOutput<AlignmentRecordDataset> toQC) {
        JavaRDD<AlignmentRecord> filtered = toQC.payload()
                .rdd()
                .toJavaRDD()
                .filter(AlignmentRecord::getReadMapped)
                .filter(read -> !read.getDuplicateRead())
                .filter(read -> read.getMappingQuality() >= 20)
                .filter(AlignmentRecord::getPrimaryAlignment)
                .filter(AlignmentRecord::getReadPaired);
        AlignmentRecordDataset AlignmentRecordDataset = toQC.payload();
        return AlignmentRecordDataset.replaceRdd(filtered.rdd(), AlignmentRecordDataset.optPartitionMap());
    }

    private static String calledBases(final String baseString) {
        return baseString.replaceAll("N", "");
    }

    private static double percentage(final double totalReads, final double readsExceedingCoverage) {
        return totalReads != 0 ? (readsExceedingCoverage / totalReads) * 100 : 0;
    }
}
