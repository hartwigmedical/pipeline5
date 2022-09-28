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
    private final boolean isDirectory;

    public AddDatatype(final DataType datatype, final String barcode, final ArchivePath path) {
        this(datatype, barcode, path, false);
    }

    public AddDatatype(final DataType datatype, final String barcode, final ArchivePath path, final boolean isDirectory) {
        this.datatype = datatype;
        this.barcode = barcode;
        this.path = path.path();
        this.isDirectory = isDirectory;
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

    public String barcode() {
        return barcode;
    }

    public boolean isDirectory() {
        return isDirectory;
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
        AddDatatype that = (AddDatatype) o;
        return isDirectory == that.isDirectory && Objects.equals(path, that.path) && datatype == that.datatype && Objects.equals(barcode,
                that.barcode);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, datatype, barcode, isDirectory);
    }
}