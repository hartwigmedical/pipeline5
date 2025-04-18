package com.hartwig.pipeline.stages;

import java.util.Collections;
import java.util.List;

import com.google.cloud.storage.Storage;
import com.hartwig.computeengine.execution.ComputeEngineStatus;
import com.hartwig.computeengine.execution.vm.BashStartupScript;
import com.hartwig.computeengine.execution.vm.ComputeEngine;
import com.hartwig.computeengine.execution.vm.RuntimeFiles;
import com.hartwig.computeengine.execution.vm.command.BashCommand;
import com.hartwig.computeengine.execution.vm.command.OutputUploadCommand;
import com.hartwig.computeengine.storage.GoogleStorageLocation;
import com.hartwig.computeengine.storage.ResultsDirectory;
import com.hartwig.computeengine.storage.RunIdentifier;
import com.hartwig.computeengine.storage.RuntimeBucket;
import com.hartwig.computeengine.storage.RuntimeBucketOptions;
import com.hartwig.pipeline.ArgumentUtil;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.PipelineStatus;
import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.failsafe.DefaultBackoffPolicy;
import com.hartwig.pipeline.input.InputMode;
import com.hartwig.pipeline.input.RunMetadata;
import com.hartwig.pipeline.labels.Labels;
import com.hartwig.pipeline.reruns.StartingPoint;
import com.hartwig.pipeline.trace.StageTrace;

import net.jodah.failsafe.Failsafe;

public class StageRunner<M extends RunMetadata> {

    private final Storage storage;
    private final Arguments arguments;
    private final VmMemoryAdjuster vmMemoryAdjuster;
    private final ComputeEngine computeEngine;
    private final ResultsDirectory resultsDirectory;
    private final StartingPoint startingPoint;
    private final Labels labels;
    private final InputMode mode;

    public StageRunner(final Storage storage, final Arguments arguments, final ComputeEngine computeEngine,
            final ResultsDirectory resultsDirectory, final StartingPoint startingPoint, final Labels labels, final InputMode mode) {
        this.storage = storage;
        this.arguments = arguments;
        this.vmMemoryAdjuster = new VmMemoryAdjuster(arguments);
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
                RunIdentifier runIdentifier = ArgumentUtil.toRunIdentifier(arguments, metadata);
                var runtimeBucketOptions = RuntimeBucketOptions.builder()
                        .namespace(stage.namespace())
                        .region(arguments.region())
                        .labels(labels.asMap())
                        .runIdentifier(runIdentifier)
                        .cmek(arguments.cmek())
                        .build();
                RuntimeBucket bucket = RuntimeBucket.from(storage, runtimeBucketOptions);
                BashStartupScript bash = BashStartupScript.of(bucket.name());
                bash.addCommands(stage.inputs())
                        .addCommands(commands)
                        .addCommand(new OutputUploadCommand(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path()),
                                RuntimeFiles.typical()));
                var jobDefinition = vmMemoryAdjuster.overrideVmDefinition(stage.vmDefinition(bash, resultsDirectory));
                ComputeEngineStatus computeEngineStatus =
                        Failsafe.with(DefaultBackoffPolicy.of(String.format("[%s] stage [%s]", metadata.runName(), stage.namespace())))
                                .get(() -> computeEngine.submit(bucket, jobDefinition));
                PipelineStatus pipelineStatus = PipelineStatus.of(computeEngineStatus);
                trace.stop();
                return stage.output(metadata, pipelineStatus, bucket, resultsDirectory);
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
