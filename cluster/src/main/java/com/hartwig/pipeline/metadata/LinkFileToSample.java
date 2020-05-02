package com.hartwig.pipeline.metadata;

import com.hartwig.pipeline.sbpapi.AddFileApiResponse;
import com.hartwig.pipeline.sbpapi.SbpRestApi;

public class LinkFileToSample implements ApiFileOperation {
    private String path;
    private final int sampleId;

    public LinkFileToSample(String path, int sampleId) {
        this.path = path;
        this.sampleId = sampleId;
    }

    @Override
    public void apply(final SbpRestApi api, final AddFileApiResponse fileResponse) {
        api.linkFileToSample(fileResponse.id(), sampleId);
    }

    @Override
    public String path() {
        return path;
    }
}
