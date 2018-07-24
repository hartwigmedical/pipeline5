package com.hartwig.pipeline;

import static java.lang.String.format;

import java.io.IOException;
import java.util.List;

import com.hartwig.io.DataSource;
import com.hartwig.io.InputOutput;
import com.hartwig.io.OutputFile;
import com.hartwig.io.OutputStore;
import com.hartwig.io.OutputType;
import com.hartwig.patient.Patient;
import com.hartwig.patient.Sample;

import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Value.Immutable
public abstract class BamCreationPipeline {

    private static final Logger LOGGER = LoggerFactory.getLogger(BamCreationPipeline.class);

    public void execute(Patient patient) throws IOException {
        LOGGER.info("Storing results in {}", OutputFile.RESULTS_DIRECTORY);
        LOGGER.info("Preprocessing started for reference sample");
        long startTime = startTimer();
        createBAM(patient.reference());
        LOGGER.info("Preprocessing complete for reference sample, Took {} ms", (endTimer() - startTime));
    }

    private void createBAM(final Sample sample) throws IOException {

        InputOutput<AlignmentRecordRDD> aligned;
        if (!bamStore().exists(sample, OutputType.ALIGNED)) {
            aligned = runStage(sample, alignment(), bamStore(), InputOutput.seed(sample));
        } else {
            skipping(alignment());
            aligned = alignmentDatasource().extract(sample);
        }

        QualityControl<AlignmentRecordRDD> readCount = qcFactory().readCount(aligned.payload());

        InputOutput<AlignmentRecordRDD> output = null;
        for (Stage<AlignmentRecordRDD, AlignmentRecordRDD> bamEnricher : bamEnrichment()) {
            if (!bamStore().exists(sample, bamEnricher.outputType())) {
                InputOutput<AlignmentRecordRDD> input = bamEnricher.datasource().extract(sample);
                qc(readCount, input);
                output = runStage(sample, bamEnricher, bamStore(), input);
            } else {
                skipping(bamEnricher);
            }
        }
        if (output != null) {
            qc(qcFactory().referenceBAMQC(), output);
        }
    }

    private void skipping(final Stage<AlignmentRecordRDD, AlignmentRecordRDD> bamEnricher) {
        LOGGER.info("Skipping [{}] stage as the output already exists in [{}]", bamEnricher.outputType(), OutputFile.RESULTS_DIRECTORY);
    }

    private void qc(final QualityControl<AlignmentRecordRDD> qcCheck, final InputOutput<AlignmentRecordRDD> toQC) throws IOException {
        QCResult check = qcCheck.check(toQC);
        if (!check.isOk()) {
            throw new IllegalStateException(check.message());
        }
    }

    private InputOutput<AlignmentRecordRDD> runStage(final Sample sample, final Stage<AlignmentRecordRDD, AlignmentRecordRDD> stage,
            final OutputStore<AlignmentRecordRDD> store, final InputOutput<AlignmentRecordRDD> input) throws IOException {
        Trace trace = Trace.of(BamCreationPipeline.class, format("Executing [%s] stage", stage.outputType())).start();
        InputOutput<AlignmentRecordRDD> output = stage.execute(input == null ? InputOutput.seed(sample) : input);
        store.store(output);
        trace.finish();
        return output;
    }

    private static long startTimer() {
        return System.currentTimeMillis();
    }

    private static long endTimer() {
        return System.currentTimeMillis();
    }

    protected abstract DataSource<AlignmentRecordRDD> alignmentDatasource();

    protected abstract AlignmentStage alignment();

    protected abstract QualityControlFactory qcFactory();

    protected abstract List<Stage<AlignmentRecordRDD, AlignmentRecordRDD>> bamEnrichment();

    protected abstract OutputStore<AlignmentRecordRDD> bamStore();

    public static ImmutableBamCreationPipeline.Builder builder() {
        return ImmutableBamCreationPipeline.builder();
    }
}
