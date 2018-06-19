package com.hartwig.pipeline;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

    private Pipeline(final List<Stage<Sample, P>> preProcessors, final OutputStore<Sample, P> perSampleStore) {
        this.preProcessors = preProcessors;
        this.perSampleStore = perSampleStore;
    }

    public void execute(RawSequencingOutput sequencing) throws IOException {
        LOGGER.info("Preprocessing started for reference sample");
        LOGGER.info("Storing results in {}", OutputFile.RESULTS_DIRECTORY);
        long startTime = startTimer();
        for (Stage<Sample, P> preProcessor : preProcessors) {
            if (!perSampleStore.exists(sequencing.patient().reference(), preProcessor.outputType())) {
                perSampleStore.store(preProcessor.execute(sequencing.patient().reference()));
            }
        }
        LOGGER.info("Preprocessing complete for reference sample, Took {} ms", (endTimer() - startTime));
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

        public Builder<P> addPreProcessingStage(Stage<Sample, P> preProcessor) {
            this.preProcessors.add(preProcessor);
            return this;
        }

        public Builder<P> perSampleStore(OutputStore<Sample, P> perSampleStore) {
            this.perSampleStore = perSampleStore;
            return this;
        }

        public Pipeline<P> build() {
            return new Pipeline<>(preProcessors, perSampleStore);
        }
    }
}
