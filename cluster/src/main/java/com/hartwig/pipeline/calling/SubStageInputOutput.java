package com.hartwig.pipeline.calling;

import java.util.Collections;
import java.util.List;

import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;

import org.immutables.value.Value;

@Value.Immutable
public interface SubStageInputOutput {

    @Value.Parameter
    String sampleName();

    @Value.Parameter
    OutputFile outputFile();

    @Value.Parameter
    List<BashCommand> bash();

    static SubStageInputOutput of(final String sampleName, final OutputFile outputFile, final List<BashCommand> bash) {
        return ImmutableSubStageInputOutput.of(sampleName, outputFile, bash);
    }

    static SubStageInputOutput seed(final String sampleName) {
        return of(sampleName, OutputFile.empty(), Collections.emptyList());
    }
}
