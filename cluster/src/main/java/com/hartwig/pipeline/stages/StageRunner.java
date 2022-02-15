package com.hartwig.pipeline.stages;

import java.util.Collections;
import java.util.List;

import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.ComputeEngine;
import com.hartwig.pipeline.execution.vm.OutputUpload;
import com.hartwig.pipeline.execution.vm.RuntimeFiles;
import com.hartwig.pipeline.failsafe.DefaultBackoffPolicy;
import com.hartwig.pipeline.labels.Labels;
import com.hartwig.pipeline.metadata.RunMetadata;
import com.hartwig.pipeline.reruns.StartingPoint;
import com.hartwig.pipeline.resource.OverrideReferenceGenomeCommand;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.trace.StageTrace;

import net.jodah.failsafe.Failsafe;

public class StageRunner<M extends RunMetadata> {

    private final Storage storage;
    private final Arguments arguments;
    private final ComputeEngine computeEngine;
    private final ResultsDirectory resultsDirectory;
    private final StartingPoint startingPoint;
    private final Labels labels;

    public StageRunner(final Storage storage, final Arguments arguments, final ComputeEngine computeEngine,
            final ResultsDirectory resultsDirectory, final StartingPoint startingPoint, final Labels labels) {
        this.storage = storage;
        this.arguments = arguments;
        this.computeEngine = computeEngine;
        this.resultsDirectory = resultsDirectory;
        this.startingPoint = startingPoint;
        this.labels = labels;
    }

    public <T extends StageOutput> T run(M metadata, Stage<T, M> stage) {
        final List<BashCommand> commands = commands(metadata, stage);
        if (stage.shouldRun(arguments) && !commands.isEmpty()) {
            if (!startingPoint.usePersisted(stage.namespace())) {
                StageTrace trace = new StageTrace(stage.namespace(), metadata.name(), StageTrace.ExecutorType.COMPUTE_ENGINE);
                RuntimeBucket bucket = RuntimeBucket.from(storage, stage.namespace(), metadata, arguments, labels);
                BashStartupScript bash = BashStartupScript.of(bucket.name());
                bash.addCommands(stage.inputs())
                        .addCommands(OverrideReferenceGenomeCommand.overrides(arguments))
                        .addCommands(commands)
                        .addCommand(new OutputUpload(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path()),
                                RuntimeFiles.typical()));
                PipelineStatus status =
                        Failsafe.with(DefaultBackoffPolicy.of(String.format("[%s] stage [%s]", metadata.name(), stage.namespace())))
                                .get(() -> computeEngine.submit(bucket, stage.vmDefinition(bash, resultsDirectory)));
                trace.stop();
                return stage.output(metadata, status, bucket, resultsDirectory);
            }
            return stage.persistedOutput(metadata);
        }
        return stage.skippedOutput(metadata);
    }

    private <T extends StageOutput> List<BashCommand> commands(final M metadata, final Stage<T, M> stage) {
        switch (metadata.mode()) {
            case SOMATIC:
                return stage.somaticCommands(metadata);
            case TUMOR_ONLY:
                return stage.tumorOnlyCommands(metadata);
            case GERMLINE_ONLY:
                return stage.germlineCommands(metadata);
            default:
                return Collections.emptyList();
        }
    }
}
