package hmf.pipeline;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import hmf.io.OutputFile;
import hmf.io.OutputStore;
import hmf.patient.RawSequencingOutput;
import hmf.patient.Sample;

public class Pipeline<P> {

    private static final Logger LOGGER = LoggerFactory.getLogger(Pipeline.class);
    private final Stage<Sample, P> preProcessor;
    private final OutputStore<Sample, P> perSampleStore;

    private Pipeline(final Stage<Sample, P> preProcessor, final OutputStore<Sample, P> perSampleStore) {
        this.preProcessor = preProcessor;
        this.perSampleStore = perSampleStore;
    }

    public void execute(RawSequencingOutput sequencing) throws IOException {
        LOGGER.info("Preprocessing started for normal sample");
        LOGGER.info("Storing results in {}", OutputFile.RESULTS_DIRECTORY);
        long startTime = startTimer();
        perSampleStore.store(preProcessor.execute(sequencing.patient().normal()));
        LOGGER.info("Preprocessing complete for normal sample, Took {} ms", (endTimer() - startTime));
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
        private Stage<Sample, P> preProcessor;
        private OutputStore<Sample, P> perSampleStore;

        public Builder<P> preProcessor(Stage<Sample, P> merge) {
            this.preProcessor = merge;
            return this;
        }

        public Builder<P> perSampleStore(OutputStore<Sample, P> perSampleStore) {
            this.perSampleStore = perSampleStore;
            return this;
        }

        public Pipeline<P> build() {
            return new Pipeline<>(preProcessor, perSampleStore);
        }
    }
}
