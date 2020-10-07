package com.hartwig.pipeline.resource;

import com.hartwig.pipeline.CommonArguments;

public class ResourceFilesFactory {

    public static ResourceFiles buildResourceFiles(final RefGenomeVersion version) {
        return version == RefGenomeVersion.HG19 ? new Hg19ResourceFiles() : new Hg38ResourceFiles();
    }

    public static ResourceFiles buildResourceFiles(final CommonArguments arguments) {
        ResourceFiles resourceFiles = buildResourceFiles(arguments.refGenomeVersion());
        return arguments.refGenomeUrl().<ResourceFiles>map(p -> new OverriddenReferenceGenome(resourceFiles, p)).orElse(resourceFiles);
    }
}