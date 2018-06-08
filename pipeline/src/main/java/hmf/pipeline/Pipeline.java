package hmf.pipeline;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import hmf.io.OutputStore;
import hmf.patient.RawSequencingOutput;
import hmf.patient.Sample;

public class Pipeline<P> {

    private static final Logger LOGGER = LoggerFactory.getLogger(Pipeline.class);
    private final Stage<Sample, P> preProcessor;
    private final OutputStore<Sample, P> perSampleStore;
    private static final String RESULTS_DIRECTORY = System.getProperty("user.dir") + "/results";

    private Pipeline(final Stage<Sample, P> preProcessor, final OutputStore<Sample, P> perSampleStore) {
        this.preProcessor = preProcessor;
        this.perSampleStore = perSampleStore;
    }

    public void execute(RawSequencingOutput sequencing) throws IOException {
        LOGGER.info("Creating results directory at [{}]", RESULTS_DIRECTORY);
        createResultsOutputDirectory();
        LOGGER.info("Preprocessing started for real sample");
        perSampleStore.store(preProcessor.execute(sequencing.patient().real()));
        LOGGER.info("Preprocessing complete for real sample");
    }

    private static void createResultsOutputDirectory() throws IOException {
        FileUtils.deleteDirectory(new File(RESULTS_DIRECTORY));
        Files.createDirectory(Paths.get(RESULTS_DIRECTORY));
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
