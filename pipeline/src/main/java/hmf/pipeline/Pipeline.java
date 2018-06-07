package hmf.pipeline;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.commons.io.FileUtils;

import hmf.sample.FlowCell;
import hmf.sample.RawSequencingOutput;

public class Pipeline {

    private final Stage<FlowCell> preProcessor;
    private static final String RESULTS_DIRECTORY = System.getProperty("user.dir") + "/results";

    private Pipeline(final Stage<FlowCell> preProcessor) {
        this.preProcessor = preProcessor;
    }

    public void execute(RawSequencingOutput sequencing) throws IOException {
        createResultsOutputDirectory();
        if (preProcessor != null) {
            preProcessor.execute(sequencing.sampled());
        }
    }

    private static void createResultsOutputDirectory() throws IOException {
        FileUtils.deleteDirectory(new File(RESULTS_DIRECTORY));
        Files.createDirectory(Paths.get(RESULTS_DIRECTORY));
    }

    public static Pipeline.Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Stage<FlowCell> preProcessor;

        public Builder preProcessor(Stage<FlowCell> merge) {
            this.preProcessor = merge;
            return this;
        }

        public Pipeline build() {
            return new Pipeline(preProcessor);
        }
    }
}
