package com.hartwig.pipeline.stages;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.ComputeEngine;
import com.hartwig.pipeline.execution.vm.OutputUpload;
import com.hartwig.pipeline.execution.vm.ResourceDownload;
import com.hartwig.pipeline.metadata.RunMetadata;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.trace.StageTrace;

public class StageRunner<M extends RunMetadata> {

    private final Storage storage;
    private final Arguments arguments;
    private final ComputeEngine computeEngine;
    private final ResultsDirectory resultsDirectory;

    public StageRunner(final Storage storage, final Arguments arguments, final ComputeEngine computeEngine,
            final ResultsDirectory resultsDirectory) {
        this.storage = storage;
        this.arguments = arguments;
        this.computeEngine = computeEngine;
        this.resultsDirectory = resultsDirectory;
    }

    public <T extends StageOutput> T run(M metadata, Stage<T, M> stage) {
        if (stage.shouldRun(arguments)) {
            StageTrace trace = new StageTrace(stage.namespace(), metadata.name(), StageTrace.ExecutorType.COMPUTE_ENGINE);
            RuntimeBucket bucket = RuntimeBucket.from(storage, stage.namespace(), metadata, arguments);
            BashStartupScript bash = BashStartupScript.of(bucket.name());
            List<ResourceDownload> resources = stage.resources(storage, arguments.resourceBucket(), bucket);
            bash.addCommands(stage.inputs())
                    .addCommands(resources)
                    .addCommands(stage.commands(metadata, resourceMap(resources)))
                    .addCommand(new OutputUpload(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path())));
            PipelineStatus status = computeEngine.submit(bucket, stage.vmDefinition(bash, resultsDirectory));
            trace.stop();
            return stage.output(metadata, status,
                    bucket,
                    resultsDirectory);
        }
        return stage.skippedOutput(metadata);
    }

    static Map<String, ResourceDownload> resourceMap(final List<ResourceDownload> resources) {
        return resources.stream()
                .collect(Collectors.toMap(resource -> resource.getResource().getName(), Function.identity()));
    }
}
