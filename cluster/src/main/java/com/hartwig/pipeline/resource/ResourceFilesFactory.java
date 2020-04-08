package com.hartwig.pipeline.resource;

public class ResourceFilesFactory {

    public static ResourceFiles buildResourceFiles(RefGenomeVersion version)
    {
        return version == RefGenomeVersion.HG37 ? new Hg37ResourceFiles() : new Hg38ResourceFiles();
    }
}
