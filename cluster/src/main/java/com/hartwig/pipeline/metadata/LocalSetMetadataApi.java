package com.hartwig.pipeline.metadata;

import com.hartwig.patient.Sample;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.execution.PipelineStatus;

public class LocalSetMetadataApi implements SetMetadataApi {

    private final Arguments arguments;

    LocalSetMetadataApi(final Arguments arguments) {
        this.arguments = arguments;
    }

    @Override
    public SetMetadata get() {

        String setId = arguments.runId().map(runId -> arguments.setId() + "-" + runId).orElse(arguments.setId());
        return SetMetadata.of(setId,
                Sample.builder("", arguments.setId() + "T").type(Sample.Type.TUMOR).build(),
                Sample.builder("", arguments.setId() + "R").type(Sample.Type.REFERENCE).build());
    }

    @Override
    public void complete(final PipelineStatus status) {
        // do nothing
    }
}