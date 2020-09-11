package com.hartwig.pipeline.metadata;

import static java.lang.String.format;

import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.sbpapi.AddFileApiResponse;
import com.hartwig.pipeline.sbpapi.SbpRestApi;

public class AddDatatypeToFile implements ApiFileOperation {
    private String path;
    private final DataType datatype;

    public AddDatatypeToFile(String path, DataType datatype) {
        this.path = path;
        this.datatype = datatype;
    }

    @Override
    public void apply(final SbpRestApi api, final AddFileApiResponse file) {
        api.patchFile(file.id(), "datatype", datatype.name().toLowerCase());
    }

    @Override
    public String path() {
        return path;
    }

    @Override
    public String toString() {
        return format("add datatype [%s] to [%s]", datatype, path);
    }
}
