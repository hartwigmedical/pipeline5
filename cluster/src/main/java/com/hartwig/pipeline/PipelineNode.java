package com.hartwig.pipeline;

import com.hartwig.pipeline.input.SomaticRunMetadata;
import com.hartwig.pipeline.stages.Stage;
import org.immutables.value.Value;

import java.util.Optional;

@Value.Immutable
public interface PipelineNode {
    Optional<StageOutput> stageOutput();
    Optional<Stage<? extends StageOutput, SomaticRunMetadata>> stage();
    String tag();
}
