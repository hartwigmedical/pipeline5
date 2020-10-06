package com.hartwig.pipeline.resource;

import com.hartwig.pipeline.CommonArguments;

public class ResourceFilesFactory {

    public static ResourceFiles buildResourceFiles(final CommonArguments arguments) {
        ResourceFiles resourceFiles =
                arguments.refGenomeVersion() == RefGenomeVersion.HG19 ? new Hg19ResourceFiles() : new Hg38ResourceFiles();
        return arguments.refGenomePath().<ResourceFiles>map(p -> new OverriddenReferenceGenome(resourceFiles, p)).orElse(resourceFiles);
    }
}
