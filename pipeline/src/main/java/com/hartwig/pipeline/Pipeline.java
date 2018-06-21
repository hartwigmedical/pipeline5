package com.hartwig.pipeline;

import static java.lang.String.format;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.hartwig.io.InputOutput;
import com.hartwig.io.OutputFile;
import com.hartwig.io.OutputStore;
import com.hartwig.patient.RawSequencingOutput;
import com.hartwig.patient.Sample;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Pipeline<P> {

    private static final Logger LOGGER = LoggerFactory.getLogger(Pipeline.class);
    private final List<Stage<Sample, P>> preProcessors;
    private final OutputStore<Sample, P> perSampleStore;
    private final boolean persistIntermediateResults;

    private Pipeline(final List<Stage<Sample, P>> preProcessors, final OutputStore<Sample, P> perSampleStore,
            final boolean persistIntermediateResults) {
        this.preProcessors = preProcessors;
        this.perSampleStore = perSampleStore;
        this.persistIntermediateResults = persistIntermediateResults;
    }

    public void execute(RawSequencingOutput sequencing) throws IOException {
        LOGGER.info("Preprocessing started for reference sample");
        LOGGER.info("Storing results in {}", OutputFile.RESULTS_DIRECTORY);
        long startTime = startTimer();
        InputOutput<Sample, P> inputOutput = null;
        boolean skippedLast = false;
        for (Stage<Sample, P> preProcessor : preProcessors) {
            if (!perSampleStore.exists(sequencing.patient().reference(), preProcessor.outputType())) {
                inputOutput = retrieveFromPersistenceIfLastSkipped(sequencing, inputOutput, skippedLast, preProcessor);
                Trace trace = Trace.of(Pipeline.class, format("Executing [%s] stage", preProcessor.getClass().getSimpleName())).start();
                inputOutput = preProcessor.execute(inputOutput == null ? InputOutput.seed(sequencing.patient().reference()) : inputOutput);
                if (persistIntermediateResults) {
                    perSampleStore.store(inputOutput);
                    trace.finish();
                }
            } else {
                LOGGER.info("Skipping [{}] stage as the output already exists in [{}]",
                        preProcessor.outputType(),
                        OutputFile.RESULTS_DIRECTORY);
                skippedLast = true;
            }
        }
        if (!persistIntermediateResults) {
            perSampleStore.store(inputOutput);
        }
        LOGGER.info("Preprocessing complete for reference sample, Took {} ms", (endTimer() - startTime));
    }

    private InputOutput<Sample, P> retrieveFromPersistenceIfLastSkipped(final RawSequencingOutput sequencing,
            InputOutput<Sample, P> inputOutput, final boolean skippedLast, final Stage<Sample, P> preProcessor) {
        if (skippedLast) {
            inputOutput = preProcessor.datasource().extract(sequencing.patient().reference());
        }
        return inputOutput;
    }

    private static long endTimer() {
        return System.currentTimeMillis();
    }

    private static long startTimer() {
        return System.currentTimeMillis();
    }

    public static <P> Pipeline.Builder<P> builder() {
        return new Builder<>();
    }

    public static class Builder<P> {
        private List<Stage<Sample, P>> preProcessors = new ArrayList<>();
        private OutputStore<Sample, P> perSampleStore;
        private boolean persistIntermediateResults = true;

        public Builder<P> addPreProcessingStage(Stage<Sample, P> preProcessor) {
            this.preProcessors.add(preProcessor);
            return this;
        }

        public Builder<P> perSampleStore(OutputStore<Sample, P> perSampleStore) {
            this.perSampleStore = perSampleStore;
            return this;
        }

        public Builder<P> persistIntermediateResults(final boolean persistIntermediateResults) {
            this.persistIntermediateResults = persistIntermediateResults;
            return this;
        }

        public Pipeline<P> build() {
            return new Pipeline<>(preProcessors, perSampleStore, persistIntermediateResults);
        }
    }
}
