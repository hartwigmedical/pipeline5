package com.hartwig.pipeline.stages;

import java.util.Collections;
import java.util.List;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.metadata.RunMetadata;
import com.hartwig.pipeline.storage.RuntimeBucket;

public interface Stage<S extends StageOutput, M extends RunMetadata> {

    List<BashCommand> inputs();

    String namespace();

    default List<BashCommand> commands(M metadata) {
        return Collections.emptyList();
    }

    default List<BashCommand> tumorOnlyCommands(M metadata) {
        return Collections.emptyList();
    }

    default List<BashCommand> germlineOnlyCommands(M metadata) {
        return Collections.emptyList();
    }

    default List<BashCommand> somaticCommands(M metadata) {
        return commands(metadata);
    }

    VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory);

    S output(final M metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket, final ResultsDirectory resultsDirectory);

    S skippedOutput(M metadata);

    default S persistedOutput(M metadata) {
        throw new UnsupportedOperationException(String.format("Stage [%s] does not support using persisted output.", namespace()));
    }

    boolean shouldRun(Arguments arguments);
}
