package com.hartwig.pipeline.output;

import com.hartwig.pipeline.report.Folder;

public class ArchivePath {

    private final Folder folder;
    private final String namespace;
    private final String filename;

    public ArchivePath(final Folder folder, final String namespace, final String filename) {
        this.folder = folder;
        this.namespace = namespace;
        this.filename = filename;
    }

    public String path() {
        return folder.name() + namespace + "/" + filename;
    }
}