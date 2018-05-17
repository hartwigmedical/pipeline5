package hmf.pipeline;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;

public class Pipeline {

    private final Map<PipelineOutput, Stage> stages;
    private static final String RESULTS_DIRECTORY = System.getProperty("user.dir") + "/results";

    private Pipeline(final Map<PipelineOutput, Stage> stages) {
        this.stages = stages;
    }

    public void execute() throws IOException {
        createResultsOutputDirectory();
        executeStage(PipelineOutput.UNMAPPED);
        executeStage(PipelineOutput.ALIGNED);
        executeStage(PipelineOutput.SORTED);
    }

    private void executeStage(final PipelineOutput pipelineOutput) throws IOException {
        Stage stage = stages.get(pipelineOutput);
        if (stage != null) {
            stage.execute();
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
        private final Map<PipelineOutput, Stage> stages = new HashMap<>();

        public Builder addStage(Stage stage) {
            stages.put(stage.output(), stage);
            return this;
        }

        public Pipeline build() {
            return new Pipeline(stages);
        }
    }
}
