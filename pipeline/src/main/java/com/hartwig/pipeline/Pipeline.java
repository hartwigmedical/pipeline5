package com.hartwig.pipeline;

import static java.lang.String.format;

import java.io.IOException;
import java.util.List;

import com.hartwig.io.InputOutput;
import com.hartwig.io.OutputFile;
import com.hartwig.io.OutputStore;
import com.hartwig.patient.Patient;
import com.hartwig.patient.Sample;

import org.immutables.value.Value;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Value.Immutable
@Value.Style(passAnnotations = { Nullable.class, NotNull.class })
public abstract class Pipeline<IN, OUT> {
    private static final Logger LOGGER = LoggerFactory.getLogger(Pipeline.class);

    @NotNull
    protected abstract List<Stage<Sample, IN, IN>> preProcessors();

    @NotNull
    protected abstract OutputStore<Sample, IN> bamStore();

    @Nullable
    protected abstract Stage<Sample, IN, OUT> germlineCalling();

    @Nullable
    protected abstract OutputStore<Sample, OUT> vcfStore();

    public void execute(Patient patient) throws IOException {
        LOGGER.info("Preprocessing started for reference sample");
        LOGGER.info("Storing results in {}", OutputFile.RESULTS_DIRECTORY);
        long startTime = startTimer();
        for (Stage<Sample, IN, IN> preProcessor : preProcessors()) {
            if (!bamStore().exists(patient.reference(), preProcessor.outputType())) {
                runStage(patient.reference(), preProcessor, bamStore());
            } else {
                LOGGER.info("Skipping [{}] stage as the output already exists in [{}]",
                        preProcessor.outputType(),
                        OutputFile.RESULTS_DIRECTORY);
            }
        }

        if (germlineCalling() != null) {
            LOGGER.info("Experimental germline calling is enabled");
            runStage(patient.reference(), germlineCalling(), vcfStore());
        }
        LOGGER.info("Preprocessing complete for reference sample, Took {} ms", (endTimer() - startTime));
    }

    private <I, O> void runStage(final Sample sample, final Stage<Sample, I, O> stage, final OutputStore<Sample, O> store)
            throws IOException {
        InputOutput<Sample, I> input = stage.datasource().extract(sample);
        Trace trace = Trace.of(Pipeline.class, format("Executing [%s] stage", stage.getClass().getSimpleName())).start();
        store.store(stage.execute(input == null ? InputOutput.seed(sample) : input));
        trace.finish();
    }

    private static long endTimer() {
        return System.currentTimeMillis();
    }

    private static long startTimer() {
        return System.currentTimeMillis();
    }
}
