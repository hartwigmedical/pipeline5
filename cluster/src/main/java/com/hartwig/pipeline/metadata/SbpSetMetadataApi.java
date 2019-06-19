package com.hartwig.pipeline.metadata;

import java.io.IOException;

import com.hartwig.patient.Sample;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.io.sbp.SBPRestApi;

public class SbpSetMetadataApi implements SetMetadataApi {

    static final String SNP_CHECK = "SnpCheck";
    static final String FAILED = "Failed";
    private final int sbpRunId;
    private final SBPRestApi sbpRestApi;

    SbpSetMetadataApi(final int sbpSetId, final SBPRestApi sbpRestApi) {
        this.sbpRunId = sbpSetId;
        this.sbpRestApi = sbpRestApi;
    }

    @Override
    public SetMetadata get() {
        try {
            SbpRun sbpRun = ObjectMappers.get().readValue(sbpRestApi.getRun(sbpRunId), SbpRun.class);
            SbpSet sbpSet = sbpRun.set();
            return SetMetadata.of(sbpSet.name(),
                    Sample.builder("", sbpSet.tumor_sample()).type(Sample.Type.TUMOR).build(),
                    Sample.builder("", sbpSet.ref_sample()).type(Sample.Type.REFERENCE).build());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void complete(final PipelineStatus status) {
        sbpRestApi.updateStatus(SBPRestApi.RUNS, String.valueOf(sbpRunId), status == PipelineStatus.SUCCESS ? SNP_CHECK : FAILED);
    }
}