package com.hartwig.pipeline.resource;

import com.hartwig.pipeline.CommonArguments;

public class ResourceFilesFactory {

    public static ResourceFiles buildResourceFiles(final RefGenomeVersion version) {
        return version == RefGenomeVersion.V37 ? new RefGenome37ResourceFiles() : new RefGenome38ResourceFiles();
    }

    public static ResourceFiles buildResourceFiles(final CommonArguments arguments) {

        ResourceFiles resourceFiles = buildResourceFiles(arguments.refGenomeVersion());

        if (arguments.refGenomeUrl().isPresent()) {
            resourceFiles = new OverriddenReferenceGenome(resourceFiles, arguments.refGenomeUrl().get());
        }

        return resourceFiles;
    }
}