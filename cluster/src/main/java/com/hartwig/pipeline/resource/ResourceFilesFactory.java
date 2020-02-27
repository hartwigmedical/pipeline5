package com.hartwig.pipeline.resource;

public class ResourceFilesFactory {

    public static Resource buildResourceFiles(RefGenomeVersion version)
    {
        return version == RefGenomeVersion.HG37 ? new Hg37Resource() : new Hg38Resource();
    }
}
