package com.hartwig.pipeline.rerun;

import java.util.List;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.metadata.RunMetadata;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.storage.RuntimeBucket;

public abstract class PersistedStage<T extends StageOutput, M extends RunMetadata> implements Stage<T, M> {

    private final Stage<T, M> decorated;
    private final String persistedBucketName;
    private final String persistedRun;
    private final StartingPoint startingPoint;

    public PersistedStage(final Stage<T, M> decorated, final Arguments arguments, final String persistedSet) {
        this(decorated, arguments.outputBucket(), new StartingPoint(arguments), persistedSet);
    }

    public PersistedStage(final Stage<T, M> decorated, final String outputBucket, final StartingPoint startingPoint,
            final String persistedSet) {
        this.decorated = decorated;
        this.persistedBucketName = outputBucket;
        this.persistedRun = persistedSet;
        this.startingPoint = startingPoint;
    }

    @Override
    public List<BashCommand> inputs() {
        return decorated.inputs();
    }

    @Override
    public String namespace() {
        return decorated.namespace();
    }

    @Override
    public List<BashCommand> commands(final M metadata) {
        return decorated.commands(metadata);
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return decorated.vmDefinition(bash, resultsDirectory);
    }

    @Override
    public T output(final M metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket, final ResultsDirectory resultsDirectory) {
        return decorated.output(metadata, jobStatus, bucket, resultsDirectory);
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        if (decorated.shouldRun(arguments)) {
            return !startingPoint.usePersisted(namespace());
        }
        return false;
    }

    protected String getPersistedBucket() {
        return persistedBucketName;
    }

    protected String getPersistedRun() {
        return persistedRun;
    }
}
