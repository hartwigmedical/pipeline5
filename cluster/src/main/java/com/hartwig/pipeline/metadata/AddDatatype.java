package com.hartwig.pipeline.metadata;

import static java.lang.String.format;

import java.util.Objects;

import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.sbpapi.AddFileApiResponse;
import com.hartwig.pipeline.sbpapi.SbpRestApi;

public class AddDatatype {
    private final String path;
    private final DataType datatype;
    private final String barcode;

    public AddDatatype(final DataType datatype, final String barcode, final ArchivePath path) {
        this.datatype = datatype;
        this.barcode = barcode;
        this.path = path.path();
    }

    public void apply(final SbpRestApi api, final AddFileApiResponse file) {
        api.patchFile(file.id(), "datatype", datatype.name().toLowerCase());
        api.linkFileToSample(file.id(), barcode);
    }

    public String path() {
        return path;
    }

    public DataType dataType() {
        return datatype;
    }

    @Override
    public String toString() {
        return format("add datatype [%s] to [%s]", datatype, path);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final AddDatatype that = (AddDatatype) o;
        return Objects.equals(path, that.path) && datatype == that.datatype && Objects.equals(barcode, that.barcode);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, datatype, barcode);
    }
}
