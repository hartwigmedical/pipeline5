package com.hartwig.pipeline.adam;

import static java.lang.String.format;

import java.util.Collection;

import com.google.common.collect.Lists;
import com.hartwig.io.InputOutput;
import com.hartwig.patient.ReferenceGenome;
import com.hartwig.pipeline.QCResult;
import com.hartwig.pipeline.QualityControl;

import org.apache.spark.api.java.JavaRDD;
import org.bdgenomics.adam.api.java.JavaADAMContext;
import org.bdgenomics.adam.models.Coverage;
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({ "FieldCanBeLocal", "unused" })
public class ADAMFinalBAMQC implements QualityControl<AlignmentRecordRDD> {

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
        JavaRDD<Coverage> coverageJavaRDD = toQC.payload().toCoverage().rdd().toJavaRDD();
        long totalCoverage = coverageJavaRDD.count();
        for (CoverageThreshold threshold : thresholds) {
            long regionExceedingCoverage = coverageJavaRDD.map(Coverage::count).filter(count -> count > threshold.coverage()).count();
            double percentageExceedingCoverage = percentage(totalCoverage, regionExceedingCoverage);
            LOGGER.info(format("BAM QC [%sx at %s%%]: Results [%s of %s at %sx coverage] or [%s%%]",
                    threshold.coverage(),
                    threshold.minimumPercentage(),
                    regionExceedingCoverage,
                    totalCoverage,
                    threshold.coverage(),
                    (long) percentageExceedingCoverage));
            if (percentageExceedingCoverage < threshold.minimumPercentage()) {
                return QCResult.failure(format(
                        "Final QC failed on sample [%s] as [%sx] " + "coverage was below the minimum minimumPercentage of [%s%%]",
                        toQC.sample().name(),
                        threshold.minimumPercentage(),
                        threshold.coverage()));
            }
        }
        return QCResult.ok();
    }

    private static double percentage(final double totalReads, final double readsExceedingCoverage) {
        return (readsExceedingCoverage / totalReads) * 100;
    }
}
