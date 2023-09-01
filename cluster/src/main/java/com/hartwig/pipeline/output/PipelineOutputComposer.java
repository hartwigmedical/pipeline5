package com.hartwig.pipeline.output;

import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.input.RunMetadata;

public interface PipelineOutputComposer {

    <T extends StageOutput> T add(final T stageOutput);

    void compose(final RunMetadata metadata, final Folder root);

}
