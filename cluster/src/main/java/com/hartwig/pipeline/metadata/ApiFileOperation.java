package com.hartwig.pipeline.metadata;

import com.hartwig.pipeline.sbpapi.AddFileApiResponse;
import com.hartwig.pipeline.sbpapi.SbpRestApi;

public interface ApiFileOperation {
    void apply(SbpRestApi api, AddFileApiResponse file);

    String path();
}
