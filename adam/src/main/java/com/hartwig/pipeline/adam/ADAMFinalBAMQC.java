package com.hartwig.pipeline.adam;

import static java.lang.String.format;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.stream.StreamSupport;

import com.google.common.collect.Lists;
import com.hartwig.io.InputOutput;
import com.hartwig.patient.ReferenceGenome;
import com.hartwig.pipeline.QCResult;
import com.hartwig.pipeline.QualityControl;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.bdgenomics.adam.api.java.JavaADAMContext;
import org.bdgenomics.adam.models.Coverage;
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD;
import org.bdgenomics.formats.avro.AlignmentRecord;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

@SuppressWarnings({ "FieldCanBeLocal", "unused" })
public class ADAMFinalBAMQC implements QualityControl<AlignmentRecordRDD>, Serializable {

    private final static Logger LOGGER = LoggerFactory.getLogger(ADAMFinalBAMQC.class);
    private final Collection<CoverageThreshold> thresholds;
    private final JavaADAMContext adamContext;
    private final ReferenceGenome referenceGenome;

    private ADAMFinalBAMQC(final Collection<CoverageThreshold> thresholds, final JavaADAMContext adamContext,
            final ReferenceGenome referenceGenome) {
        this.thresholds = thresholds;
        this.adamContext = adamContext;
        this.referenceGenome = referenceGenome;
    }

    public static ADAMFinalBAMQC of(final JavaADAMContext adamContext, final ReferenceGenome referenceGenome,
            final CoverageThreshold... thresholds) {
        return new ADAMFinalBAMQC(Lists.newArrayList(thresholds), adamContext, referenceGenome);
    }

    @Override
    public QCResult check(final InputOutput<AlignmentRecordRDD> toQC) {
        if (toQC.payload().rdd().isEmpty()) {
            return QCResult.failure("Final QC failed as the BAM was empty");
        }

        JavaPairRDD<String, Integer> calledBasesPerSequence = adamContext.ac()
                .loadFasta(referenceGenome.path(), Long.MAX_VALUE)
                .rdd()
                .toJavaRDD()
                .mapToPair(fragment -> Tuple2.apply(fragment.getContigName(), calledBases(fragment.getSequence()).length()));
        AlignmentRecordRDD filterReads = filterReads(toQC);

        for (CoverageThreshold threshold : thresholds) {
            List<CoverageMetrics> metrics = CoverageRDD.toCoverage(filterReads)
                    .keyBy(Coverage::contigName)
                    .groupByKey()
                    .mapToPair(contigAndQualityCount(threshold.coverage()))
                    .join(calledBasesPerSequence)
                    .map(toCoverageMetrics())
                    .collect();

            long totalExceeding = 0;
            long total = 0;
            for (CoverageMetrics metric : metrics) {
                totalExceeding += metric.exceeding();
                total += metric.total();
            }
            double percentage = percentage(total, totalExceeding);
            LOGGER.info(format("BAM QC [%sx at %f%%]: Results [%s of %s at %dx coverage] or [%f%%]",
                    threshold.coverage(), threshold.minimumPercentage(), totalExceeding, total, threshold.coverage(), percentage));
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
    private static Function<Tuple2<String, Tuple2<Long, Integer>>, CoverageMetrics> toCoverageMetrics() {
        return tuple -> CoverageMetrics.of(tuple._1, tuple._2._1, tuple._2._2);
    }

    @NotNull
    private static PairFunction<Tuple2<String, Iterable<Coverage>>, String, Long> contigAndQualityCount(final double coverageThreshold) {
        return tuple -> {
            Iterable<Coverage> coverages = tuple._2;
            long regionExceedingCoverage = StreamSupport.stream(coverages.spliterator(), false)
                    .map(Coverage::count)
                    .filter(count -> count > coverageThreshold)
                    .count();
            return Tuple2.apply(tuple._1, regionExceedingCoverage);
        };
    }

    @NotNull
    private static AlignmentRecordRDD filterReads(final InputOutput<AlignmentRecordRDD> toQC) {
        JavaRDD<AlignmentRecord> filtered = toQC.payload()
                .rdd()
                .toJavaRDD()
                .filter(read -> !read.getDuplicateRead())
                .filter(AlignmentRecord::getReadMapped).filter(read -> read.getMapq() >= 20)
                .filter(AlignmentRecord::getPrimaryAlignment);
        AlignmentRecordRDD alignmentRecordRDD = toQC.payload();
        return alignmentRecordRDD.replaceRdd(filtered.rdd(), alignmentRecordRDD.optPartitionMap());
    }

    private static String calledBases(final String baseString) {
        return baseString.replaceAll("N", "");
    }

    private static double percentage(final double totalReads, final double readsExceedingCoverage) {
        return totalReads != 0 ? (readsExceedingCoverage / totalReads) * 100 : 0;
    }
}
