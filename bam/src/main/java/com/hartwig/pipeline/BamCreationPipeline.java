package com.hartwig.pipeline;

import static java.lang.String.format;

import java.io.IOException;

import com.hartwig.io.DataSource;
import com.hartwig.io.InputOutput;
import com.hartwig.io.OutputStore;
import com.hartwig.io.OutputType;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.after.BamIndexPipeline;
import com.hartwig.pipeline.metrics.Metric;
import com.hartwig.pipeline.metrics.Monitor;

import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Value.Immutable
public abstract class BamCreationPipeline {

    private static final Logger LOGGER = LoggerFactory.getLogger(BamCreationPipeline.class);
    static final String BAM_CREATED_METRIC = "BAM_CREATED";

    public void execute(final Sample sample) {
        LOGGER.info("Preprocessing started for {} sample", sample.name());
        StatusReporter.Status status = StatusReporter.Status.SUCCESS;
        try {
            long startTime = startTimer();
            QCResult qcResult;
            if (finalBamStore().exists(sample, OutputType.FINAL)) {
                LOGGER.info("BAM for {} sample already exists. Only running QC", sample.name());
                qcResult = qc(finalQC(), finalDatasource().extract(sample));
            } else {
                InputOutput<AlignmentRecordRDD> aligned = alignment().execute(InputOutput.seed(sample));
                InputOutput<AlignmentRecordRDD> enriched = bamEnrichment().execute(aligned);
                qcResult = qc(finalQC(), enriched);
                finalBamStore().store(enriched);
            }
            if (!qcResult.isOk()) {
                status = StatusReporter.Status.FAILED_FINAL_QC;
            }
            long timeSpent = endTimer() - startTime;
            LOGGER.info("Preprocessing complete for {} sample, Took {} ms", sample.name(), timeSpent);
            monitor().update(Metric.spentTime(BAM_CREATED_METRIC, timeSpent));

        } catch (Exception e) {
            LOGGER.error(format("Unable to create BAM for %s. Check exception for details", sample.name()), e);
            if (status == StatusReporter.Status.SUCCESS) {
                status = StatusReporter.Status.FAILED_ERROR;
            }
            throw new RuntimeException(e);
        } finally {
            statusReporter().report(status);
        }
    }

    private QCResult qc(final QualityControl<AlignmentRecordRDD> qcCheck, final InputOutput<AlignmentRecordRDD> toQC) {
        return qcCheck.check(toQC);
    }

    private static long startTimer() {
        return System.currentTimeMillis();
    }

    private static long endTimer() {
        return System.currentTimeMillis();
    }

    protected abstract DataSource<AlignmentRecordRDD> finalDatasource();

    protected abstract AlignmentStage alignment();

    protected abstract Stage<AlignmentRecordRDD, AlignmentRecordRDD> bamEnrichment();

    protected abstract OutputStore<AlignmentRecordRDD> finalBamStore();

    protected abstract QualityControl<AlignmentRecordRDD> finalQC();

    protected abstract BamIndexPipeline indexBam();

    protected abstract Monitor monitor();

    protected abstract StatusReporter statusReporter();

    public static ImmutableBamCreationPipeline.Builder builder() {
        return ImmutableBamCreationPipeline.builder();
    }
}
