package com.hartwig.pipeline.metadata;

import com.hartwig.pipeline.sbpapi.FileResponse;
import com.hartwig.pipeline.sbpapi.SbpRestApi;

public interface ApiFileOperation {
    void apply(SbpRestApi api, FileResponse file);
}
