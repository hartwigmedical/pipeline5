package hmf.pipeline;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.commons.io.FileUtils;

import hmf.patient.RawSequencingOutput;
import hmf.patient.Sample;

public class Pipeline {

    private final Stage<Sample> preProcessor;
    private static final String RESULTS_DIRECTORY = System.getProperty("user.dir") + "/results";

    private Pipeline(final Stage<Sample> preProcessor) {
        this.preProcessor = preProcessor;
    }

    public void execute(RawSequencingOutput sequencing) throws IOException {
        createResultsOutputDirectory();
        if (preProcessor != null) {
            preProcessor.execute(sequencing.patient().real());
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
        private Stage<Sample> preProcessor;

        public Builder preProcessor(Stage<Sample> merge) {
            this.preProcessor = merge;
            return this;
        }

        public Pipeline build() {
            return new Pipeline(preProcessor);
        }
    }
}
