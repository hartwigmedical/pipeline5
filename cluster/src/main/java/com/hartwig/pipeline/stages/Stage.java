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
import com.hartwig.pipeline.input.RunMetadata;
import com.hartwig.pipeline.output.AddDatatype;
import com.hartwig.pipeline.storage.RuntimeBucket;

public interface Stage<S extends StageOutput, M extends RunMetadata> {

    List<BashCommand> inputs();

    String namespace();

    default List<BashCommand> commands(final M metadata) {
        return disabled();
    }

    default List<BashCommand> tumorOnlyCommands(final M metadata) {
        return disabled();
    }

    default List<BashCommand> referenceOnlyCommands(final M metadata) {
        return disabled();
    }

    default List<BashCommand> tumorReferenceCommands(final M metadata) {
        return commands(metadata);
    }

    VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory);

    S output(final M metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket, final ResultsDirectory resultsDirectory);

    S skippedOutput(M metadata);

    default S persistedOutput(final M metadata) {
        throw new UnsupportedOperationException(String.format("Stage [%s] does not support using persisted output.", namespace()));
    }

    default List<AddDatatype> addDatatypes(final M metadata) {
        throw new UnsupportedOperationException(String.format("Stage [%s] has not defined any datatypes to add.", namespace()));
    }

    boolean shouldRun(Arguments arguments);

    static List<BashCommand> disabled() {
        return Collections.emptyList();
    }
}
