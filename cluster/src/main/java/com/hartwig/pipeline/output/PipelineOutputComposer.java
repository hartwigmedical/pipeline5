package com.hartwig.pipeline.output;

import com.hartwig.computeengine.input.RunMetadata;
import com.hartwig.pipeline.StageOutput;

public interface PipelineOutputComposer {

    <T extends StageOutput> T add(final T stageOutput);

    void compose(final RunMetadata metadata, final Folder root);

}
