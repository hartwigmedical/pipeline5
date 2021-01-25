package com.hartwig.pipeline.metadata;

import static java.lang.String.format;

import java.util.Objects;

import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.sbpapi.AddFileApiResponse;
import com.hartwig.pipeline.sbpapi.SbpRestApi;

public class AddDatatypeToFile implements ApiFileOperation {
    private final String path;
    private final DataType datatype;
    private final String barcode;
    private final boolean isDirectory;

    private AddDatatypeToFile(final DataType datatype, final Folder folder, final String namespace, final String filename,
            final String barcode) {
        this.datatype = datatype;
        this.barcode = barcode;
        String namespacedFile = namespace;
        if (filename != null) {
            isDirectory = false;
            namespacedFile = namespace + "/" + filename;
        } else {
            isDirectory = true;
        }
        path = folder.name().isEmpty() ? namespacedFile : folder.name() + namespacedFile;
    }

    public static AddDatatypeToFile file(final DataType datatype, final Folder folder, final String namespace, final String filename,
            final String barcode) {
        if (filename.isEmpty()) {
            throw new IllegalArgumentException("Empty filename not supported for file");
        }
        return new AddDatatypeToFile(datatype, folder, namespace, filename, barcode);
    }

    public static AddDatatypeToFile directory(final DataType datatype, final Folder folder, final String directory, final String barcode) {
        return new AddDatatypeToFile(datatype, folder, directory, null, barcode);
    }

    @Override
    public void apply(final SbpRestApi api, final AddFileApiResponse file) {
        api.patchFile(file.id(), "datatype", datatype.name().toLowerCase());
        api.linkFileToSample(file.id(), barcode);
    }

    @Override
    public String path() {
        return path;
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
        final AddDatatypeToFile that = (AddDatatypeToFile) o;
        return Objects.equals(path, that.path) && datatype == that.datatype && Objects.equals(barcode, that.barcode);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, datatype, barcode);
    }
}
