package com.hartwig.pipeline;

import static java.lang.String.format;

import com.hartwig.io.DataSource;
import com.hartwig.io.InputOutput;
import com.hartwig.io.OutputStore;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.metrics.Metric;
import com.hartwig.pipeline.metrics.Monitor;

import org.bdgenomics.adam.rdd.read.AlignmentRecordDataset;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Value.Immutable
public abstract class BamCreationPipeline {

    private static final Logger LOGGER = LoggerFactory.getLogger(BamCreationPipeline.class);
    static final String BAM_CREATED_METRIC = "BAM_CREATED";
    private static final String RECALIBRATED_SUFFIX = "recalibrated";

    public void execute(final Sample sample) {
        LOGGER.info("Clearing result directory before starting");
        finalBamStore().clear();
        LOGGER.info("Preprocessing started for {} sample", sample.name());
        StatusReporter.Status status = StatusReporter.Status.SUCCESS;
        try {
            long startTime = startTimer();
            QCResult qcResult;
            if (finalBamStore().exists(sample)) {
                LOGGER.info("BAM for {} sample already exists. Only running QC", sample.name());
                qcResult = qc(finalQC(), finalDatasource().extract(sample));
            } else {
                InputOutput<AlignmentRecordDataset> aligned = alignment().execute(InputOutput.seed(sample));
                InputOutput<AlignmentRecordDataset> enriched = markDuplicates().execute(aligned);
                InputOutput<AlignmentRecordDataset> recalibrated = recalibration().execute(enriched);
                qcResult = qc(finalQC(), enriched);
                LOGGER.info("Storing enriched BAM");
                finalBamStore().store(enriched);
                LOGGER.info("Enriched BAM stored. Storing recalibrated BAM");
                finalBamStore().store(recalibrated, RECALIBRATED_SUFFIX);
                LOGGER.info("Recalibrated BAM stored");
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

    private QCResult qc(final QualityControl<AlignmentRecordDataset> qcCheck, final InputOutput<AlignmentRecordDataset> toQC) {
        return qcCheck.check(toQC);
    }

    private static long startTimer() {
        return System.currentTimeMillis();
    }

    private static long endTimer() {
        return System.currentTimeMillis();
    }

    protected abstract DataSource<AlignmentRecordDataset> finalDatasource();

    protected abstract AlignmentStage alignment();

    protected abstract Stage<AlignmentRecordDataset, AlignmentRecordDataset> markDuplicates();

    protected abstract Stage<AlignmentRecordDataset, AlignmentRecordDataset> recalibration();

    protected abstract OutputStore<AlignmentRecordDataset> finalBamStore();

    protected abstract QualityControl<AlignmentRecordDataset> finalQC();

    protected abstract Monitor monitor();

    protected abstract StatusReporter statusReporter();

    public static ImmutableBamCreationPipeline.Builder builder() {
        return ImmutableBamCreationPipeline.builder();
    }
}
