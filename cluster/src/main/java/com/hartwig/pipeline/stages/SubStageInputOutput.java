package com.hartwig.pipeline.stages;

import com.hartwig.pipeline.storage.OutputFile;
import com.hartwig.computeengine.execution.vm.command.BashCommand;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Value.Immutable
public interface SubStageInputOutput {

    @Value.Parameter
    String sampleName();

    @Value.Parameter
    OutputFile outputFile();

    @Value.Parameter
    List<BashCommand> bash();

    default SubStageInputOutput combine(final SubStageInputOutput subStageInputOutput) {
        List<BashCommand> commands = new ArrayList<>();
        commands.addAll(bash());
        commands.addAll(subStageInputOutput.bash());
        return SubStageInputOutput.of(sampleName(), outputFile(), commands);
    }

    static SubStageInputOutput of(final String sampleName, final OutputFile outputFile, final List<BashCommand> bash) {
        return ImmutableSubStageInputOutput.of(sampleName, outputFile, bash);
    }

    static SubStageInputOutput empty(final String sampleName) {
        return of(sampleName, OutputFile.empty(), Collections.emptyList());
    }
}
