package com.hartwig.pipeline.metadata;

import com.hartwig.pipeline.sbpapi.FileResponse;
import com.hartwig.pipeline.sbpapi.SbpRestApi;

public class AddDatatypeToFile implements ApiFileOperation {
    private final String datatype;

    public AddDatatypeToFile(String datatype) {
        this.datatype = datatype;
    }

    @Override
    public void apply(final SbpRestApi api, final FileResponse file) {
        api.patchFile(file.id, "datatype", datatype);
    }
}
