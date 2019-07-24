package com.hartwig.bam;

import static java.lang.String.format;

import com.hartwig.io.DataSource;
import com.hartwig.io.InputOutput;
import com.hartwig.io.OutputStore;
import com.hartwig.patient.Sample;

import org.bdgenomics.adam.rdd.read.AlignmentRecordDataset;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Value.Immutable
public abstract class BamCreationPipeline {

    private static final Logger LOGGER = LoggerFactory.getLogger(BamCreationPipeline.class);

    public void execute(final Sample sample) {
        LOGGER.info("Clearing result directory before starting");
        finalBamStore().clear();
        LOGGER.info("ADAM BAM creation started for sample [{}]", sample.name());
        StatusReporter.Status status = StatusReporter.Status.SUCCESS;
        try {
            InputOutput<AlignmentRecordDataset> aligned = executeAndCache(InputOutput.seed(sample), alignment());
            QCResult qcResult = qc(finalQC(), aligned);
            if (qcResult.isOk()) {
                finalBamStore().store(executeAndCache(aligned, markDuplicates()));
            }
            if (!qcResult.isOk()) {
                status = StatusReporter.Status.FAILED_FINAL_QC;
            }
            LOGGER.info("ADAM BAM creation completed for sample [{}]", sample.name());
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

    private InputOutput<AlignmentRecordDataset> executeAndCache(final InputOutput<AlignmentRecordDataset> input,
            final Stage<AlignmentRecordDataset, AlignmentRecordDataset> stage) throws java.io.IOException {
        InputOutput<AlignmentRecordDataset> result = stage.execute(input);
        result.payload().cache();
        return result;
    }

    private QCResult qc(final QualityControl<AlignmentRecordDataset> qcCheck, final InputOutput<AlignmentRecordDataset> toQC) {
        QCResult qcResult = qcCheck.check(toQC);
        if (!qcResult.isOk()) {
            LOGGER.error("QC failed for [{}] stage with reason [{}]", "alignment (bwa mem)", qcResult.message());
        }
        return qcResult;
    }

    protected abstract DataSource<AlignmentRecordDataset> finalDatasource();

    protected abstract AlignmentStage alignment();

    protected abstract Stage<AlignmentRecordDataset, AlignmentRecordDataset> markDuplicates();

    protected abstract OutputStore<AlignmentRecordDataset> finalBamStore();

    protected abstract QualityControl<AlignmentRecordDataset> finalQC();

    protected abstract StatusReporter statusReporter();

    public static ImmutableBamCreationPipeline.Builder builder() {
        return ImmutableBamCreationPipeline.builder();
    }
}
