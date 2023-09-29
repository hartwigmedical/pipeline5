package com.hartwig.pipeline.stages;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.cloud.storage.Storage;
import com.hartwig.computeengine.execution.vm.BashStartupScript;
import com.hartwig.computeengine.execution.vm.ComputeEngine;
import com.hartwig.computeengine.execution.vm.RuntimeFiles;
import com.hartwig.computeengine.execution.vm.command.BashCommand;
import com.hartwig.computeengine.execution.vm.command.OutputUploadCommand;
import com.hartwig.computeengine.storage.GoogleStorageLocation;
import com.hartwig.computeengine.storage.ResultsDirectory;
import com.hartwig.computeengine.storage.RunIdentifier;
import com.hartwig.computeengine.storage.RuntimeBucket;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.PipelineStatus;
import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.failsafe.DefaultBackoffPolicy;
import com.hartwig.pipeline.input.InputMode;
import com.hartwig.pipeline.input.RunMetadata;

import com.hartwig.pipeline.reruns.StartingPoint;
import com.hartwig.pipeline.storage.StorageUtil;
import com.hartwig.pipeline.trace.StageTrace;

import net.jodah.failsafe.Failsafe;

public class StageRunner<M extends RunMetadata> {

    private final Storage storage;
    private final Arguments arguments;
    private final ComputeEngine computeEngine;
    private final ResultsDirectory resultsDirectory;
    private final StartingPoint startingPoint;
    private final Map<String, String> labels;
    private final InputMode mode;

    public StageRunner(final Storage storage, final Arguments arguments, final ComputeEngine computeEngine,
                       final ResultsDirectory resultsDirectory, final StartingPoint startingPoint, final Map<String, String> labels, final InputMode mode) {
        this.storage = storage;
        this.arguments = arguments;
        this.computeEngine = computeEngine;
        this.resultsDirectory = resultsDirectory;
        this.startingPoint = startingPoint;
        this.labels = labels;
        this.mode = mode;
    }

    public <T extends StageOutput> T run(final M metadata, final Stage<T, M> stage) {
        final List<BashCommand> commands = commands(mode, metadata, stage);
        if (stage.shouldRun(arguments) && !commands.isEmpty()) {
            if (!startingPoint.usePersisted(stage.namespace())) {
                StageTrace trace = new StageTrace(stage.namespace(), metadata.runName(), StageTrace.ExecutorType.COMPUTE_ENGINE);
                RunIdentifier runIdentifier = StorageUtil.runIdentifierFromArguments(metadata, arguments);
                RuntimeBucket bucket = RuntimeBucket.from(storage,
                        stage.namespace(),
                        arguments.region(),
                        labels,
                        runIdentifier,
                        arguments.cmek().orElse(null));
                BashStartupScript bash = BashStartupScript.of(bucket.name());
                bash.addCommands(stage.inputs())
                        .addCommands(commands)
                        .addCommand(new OutputUploadCommand(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path()),
                                RuntimeFiles.typical()));
                PipelineStatus status =
                        Failsafe.with(DefaultBackoffPolicy.of(String.format("[%s] stage [%s]", metadata.runName(), stage.namespace())))
                                .get(() -> PipelineStatus.of(computeEngine.submit(bucket, stage.vmDefinition(bash, resultsDirectory))));
                trace.stop();
                return stage.output(metadata, status, bucket, resultsDirectory);
            }
            return stage.persistedOutput(metadata);
        }
        return stage.skippedOutput(metadata);
    }

    private <T extends StageOutput> List<BashCommand> commands(final InputMode mode, final M metadata, final Stage<T, M> stage) {
        switch (mode) {
            case TUMOR_REFERENCE:
                return stage.tumorReferenceCommands(metadata);
            case TUMOR_ONLY:
                return stage.tumorOnlyCommands(metadata);
            case REFERENCE_ONLY:
                return stage.referenceOnlyCommands(metadata);
            default:
                return Collections.emptyList();
        }
    }
}
