package com.hartwig.bcl2fastq.metadata;

import com.hartwig.bcl2fastq.conversion.Conversion;
import com.hartwig.bcl2fastq.conversion.ConvertedFastq;
import com.hartwig.bcl2fastq.conversion.ConvertedSample;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QualityControl {

    private static final Logger LOGGER = LoggerFactory.getLogger(QualityControl.class);
    private static final String FAILED_QC = "Failed QC: ";
    private static final int MIN_UNDETERMINED_READ_PERCENTAGE = 8;
    private static final int ONE_GIGABASE = 1_000_000_000;
    private static final long MIN_YIELD_PER_SAMPLE = ONE_GIGABASE;

    static boolean errorsInLogs(String log) {
        if (!log.contains("with 0 errors and")) {
            LOGGER.warn(FAILED_QC + "There were errors in the conversion log files which require investigation.");
            return false;
        }
        return true;
    }

    static boolean undeterminedReadPercentage(final double percUndeterminedYield) {
        if (percUndeterminedYield > MIN_UNDETERMINED_READ_PERCENTAGE) {
            LOGGER.warn(FAILED_QC + "Undetermined read percentage of [{}] was higher than the minimum of [{}]",
                    percUndeterminedYield,
                    MIN_UNDETERMINED_READ_PERCENTAGE);
            return false;
        }
        return true;
    }

    static boolean minimumYield(Conversion conversion) {
        boolean noSamplesFail = true;
        for (ConvertedSample sample : conversion.samples()) {
            if (sample.yield() < MIN_YIELD_PER_SAMPLE) {
                LOGGER.warn(FAILED_QC + "Sample [{}] had a yield of [{}] lower than the minimum of [{}]",
                        sample.barcode(),
                        sample.yield(),
                        MIN_YIELD_PER_SAMPLE);
                noSamplesFail = false;
            }
        }
        return noSamplesFail;
    }

    static boolean minimumQ30(ConvertedFastq fastq, double minQ30Required) {
        if (Q30.of(fastq) < minQ30Required) {
            LOGGER.warn(FAILED_QC + "FastQ for [{}] did not meet the minimum Q30 requirement of [{}]", fastq.id(), minQ30Required);
            return false;
        }
        return true;
    }
}
