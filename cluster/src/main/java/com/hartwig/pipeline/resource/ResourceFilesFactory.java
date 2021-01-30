package com.hartwig.pipeline.resource;

import com.hartwig.pipeline.CommonArguments;

public class ResourceFilesFactory {

    public static ResourceFiles buildResourceFiles(final RefGenomeVersion version) {
        switch (version) {
            case V37:
                return new RefGenome37ResourceFiles();
            case V38:
                return new RefGenome38ResourceFiles();
            case V38_ALT:
                return new RefGenome38PlusAltResourceFiles();
        }
        throw new IllegalArgumentException();
    }

    public static ResourceFiles buildResourceFiles(final CommonArguments arguments) {
        ResourceFiles resourceFiles = buildResourceFiles(arguments.refGenomeVersion());
        return arguments.refGenomeUrl().<ResourceFiles>map(p -> new OverriddenReferenceGenome(resourceFiles, p)).orElse(resourceFiles);
    }
}