package com.hartwig.pipeline.metadata;

import java.io.IOException;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.io.sbp.SBPRestApi;

public class SbpSetMetadataApi implements SetMetadataApi {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    static {
        OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    private final int sbpRunId;
    private final SBPRestApi sbpRestApi;

    SbpSetMetadataApi(final int sbpSetId, final SBPRestApi sbpRestApi) {
        this.sbpRunId = sbpSetId;
        this.sbpRestApi = sbpRestApi;
    }

    @Override
    public SetMetadata get() {
        try {
            SbpRun sbpRun = OBJECT_MAPPER.readValue(sbpRestApi.getRun(sbpRunId), SbpRun.class);
            SbpSet sbpSet = sbpRun.set();
            return SetMetadata.of(sbpSet.name(),
                    Sample.builder("", sbpSet.tumor_sample()).type(Sample.Type.TUMOR).build(),
                    Sample.builder("", sbpSet.ref_sample()).type(Sample.Type.REFERENCE).build());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
