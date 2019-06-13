package com.hartwig.pipeline.metadata;

import com.hartwig.patient.Sample;

public class LocalSetMetadataApi implements SetMetadataApi {

    private final String setId;

    LocalSetMetadataApi(final String setId) {
        this.setId = setId;
    }

    @Override
    public SetMetadata get() {
        return SetMetadata.of(setId,
                Sample.builder("", setId + "T").type(Sample.Type.TUMOR).build(),
                Sample.builder("", setId + "R").type(Sample.Type.REFERENCE).build());
    }
}
