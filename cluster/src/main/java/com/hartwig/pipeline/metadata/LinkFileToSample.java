package com.hartwig.pipeline.metadata;

import static java.lang.String.format;

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

    @Override
    public String toString() {
        return format("link [%s] to sample [%d]", path, sampleId);
    }
}
