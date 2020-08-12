package com.hartwig.pipeline.resource;

public class ResourceFilesFactory {

    public static ResourceFiles buildResourceFiles(RefGenomeVersion version)
    {
        return version == RefGenomeVersion.HG19 ? new Hg19ResourceFiles() : new Hg38ResourceFiles();
    }
}
