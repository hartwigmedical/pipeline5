package hmf.pipeline;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.commons.io.FileUtils;

import hmf.io.OutputStore;
import hmf.patient.RawSequencingOutput;
import hmf.patient.Sample;

public class Pipeline<P> {

    private final Stage<Sample, P> preProcessor;
    private final OutputStore<Sample, P> perSampleStore;
    private static final String RESULTS_DIRECTORY = System.getProperty("user.dir") + "/results";

    private Pipeline(final Stage<Sample, P> preProcessor, final OutputStore<Sample, P> perSampleStore) {
        this.preProcessor = preProcessor;
        this.perSampleStore = perSampleStore;
    }

    public void execute(RawSequencingOutput sequencing) throws IOException {
        createResultsOutputDirectory();
        if (preProcessor != null) {
            perSampleStore.store(preProcessor.execute(sequencing.patient().real()));
        }
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
