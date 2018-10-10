package com.hartwig.pipeline;

import static java.lang.String.format;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import com.hartwig.io.DataSource;
import com.hartwig.io.InputOutput;
import com.hartwig.io.OutputStore;
import com.hartwig.io.OutputType;
import com.hartwig.patient.Sample;
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
            QCResult qcResult;
            long startTime = startTimer();
            if (finalBamStore().exists(sample, OutputType.FINAL)) {
                LOGGER.info("BAM for {} sample already exists. Only running QC", sample.name());
                qcResult = qc(finalQC(), finalDatasource().extract(sample));
            } else {
                InputOutput<AlignmentRecordRDD> aligned = runStage(sample, alignment(), InputOutput.seed(sample));
                QualityControl<AlignmentRecordRDD> readCount = readCountQCFactory().apply(aligned.payload());
                InputOutput<AlignmentRecordRDD> enriched = runStage(sample, bamEnrichment(), aligned);
                qcResult = qc(readCount, enriched);
                if (!qcResult.isOk()) {
                    status = StatusReporter.Status.FAILED_READ_COUNT;
                    throw new IllegalStateException(String.format("QC failed on stage [%s] with message [%s] ",
                            bamEnrichment().outputType(),
                            qcResult.message()));
                }
                qcResult = qc(finalQC(), enriched);
                finalBamStore().store(enriched);
                indexBam().execute(sample);
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

    private QCResult qc(final QualityControl<AlignmentRecordRDD> qcCheck, final InputOutput<AlignmentRecordRDD> toQC) throws IOException {
        return qcCheck.check(toQC);
    }

    private InputOutput<AlignmentRecordRDD> runStage(final Sample sample, final Stage<AlignmentRecordRDD, AlignmentRecordRDD> stage,
            final InputOutput<AlignmentRecordRDD> input) throws IOException {
        Trace trace =
                Trace.of(BamCreationPipeline.class, format("Executing [%s] stage for [%s]", stage.outputType(), sample.name())).start();
        InputOutput<AlignmentRecordRDD> output = stage.execute(input == null ? InputOutput.seed(sample) : input);
        trace.finish();
        monitor().update(Metric.spentTime(stage.outputType().name(), trace.getExecutionTime()));
        return output;
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

    protected abstract Function<AlignmentRecordRDD, QualityControl<AlignmentRecordRDD>> readCountQCFactory();

    protected abstract QualityControl<AlignmentRecordRDD> finalQC();

    protected abstract IndexBam indexBam();

    protected abstract ExecutorService executorService();

    protected abstract Monitor monitor();

    protected abstract StatusReporter statusReporter();

    public static ImmutableBamCreationPipeline.Builder builder() {
        return ImmutableBamCreationPipeline.builder();
    }
}
