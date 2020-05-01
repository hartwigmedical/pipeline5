package com.hartwig.pipeline.metadata;

import com.hartwig.pipeline.sbpapi.FileResponse;
import com.hartwig.pipeline.sbpapi.SbpRestApi;

public class LinkFileToSample implements ApiFileOperation {
    private final String sampleId;

    public LinkFileToSample(String sampleId) {
        this.sampleId = sampleId;
    }

    @Override
    public void apply(final SbpRestApi api, final FileResponse fileResponse) {
        api.linkFileToSample(fileResponse.id, sampleId);
    }
}
