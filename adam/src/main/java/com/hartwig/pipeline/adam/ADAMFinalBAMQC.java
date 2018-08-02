package com.hartwig.pipeline.adam;

import static java.lang.String.format;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.Lists;
import com.hartwig.io.InputOutput;
import com.hartwig.patient.ReferenceGenome;
import com.hartwig.pipeline.QCResult;
import com.hartwig.pipeline.QualityControl;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import org.bdgenomics.adam.api.java.JavaADAMContext;
import org.bdgenomics.adam.models.Coverage;
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD;
import org.bdgenomics.formats.avro.AlignmentRecord;
import org.jetbrains.annotations.NotNull;
import org.seqdoop.hadoop_bam.SAMRecordWritable;
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

        Map<String, Integer> calledBasesPerContig = adamContext.ac()
                .loadFasta(referenceGenome.path(), Long.MAX_VALUE)
                .rdd()
                .toJavaRDD()
                .mapToPair(fragment -> Tuple2.apply(fragment.getContigName(), calledBases(fragment.getSequence()).length()))
                .collectAsMap();

        for (CoverageThreshold threshold : thresholds) {

            Map<String, CoverageMetrics> metricsPerContig = new HashMap<>();
            for (String contigName : calledBasesPerContig.keySet()) {

                AlignmentRecordRDD filterReads = filterReads(toQC, contigName);
                RDD<SAMRecordWritable> samRecordRDD = filterReads.convertToSam(false)._1;
                long countExceedingThreshold = CoverageRDD.toCoverage(contigName, samRecordRDD)
                        .mapToDouble(Coverage::count)
                        .filter(count -> count > threshold.coverage())
                        .count();
                metricsPerContig.put(contigName,
                        CoverageMetrics.of(contigName, countExceedingThreshold, calledBasesPerContig.get(contigName)));
            }

            long totalExceeding = 0;
            long total = 0;
            for (CoverageMetrics metric : metricsPerContig.values()) {
                totalExceeding += metric.exceeding();
                total += metric.total();
            }
            double percentage = percentage(total, totalExceeding);
            LOGGER.info(format("BAM QC [%sx at %f%%]: Results [%s of %s at %dx coverage] or [%f%%]",
                    threshold.coverage(),
                    threshold.minimumPercentage(),
                    totalExceeding,
                    total,
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
    private static Function<Tuple2<String, Tuple2<Long, Integer>>, CoverageMetrics> toCoverageMetrics() {
        return tuple -> CoverageMetrics.of(tuple._1, tuple._2._1, tuple._2._2);
    }

    @NotNull
    private static AlignmentRecordRDD filterReads(final InputOutput<AlignmentRecordRDD> toQC, final String contigName) {
        JavaRDD<AlignmentRecord> filtered = toQC.payload()
                .rdd()
                .toJavaRDD().filter(read -> read.getContigName().equals(contigName))
                .filter(read -> !read.getDuplicateRead())
                .filter(AlignmentRecord::getReadMapped)
                .filter(read -> read.getMapq() >= 20)
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
