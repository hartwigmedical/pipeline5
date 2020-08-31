package com.hartwig.pipeline.resource;

import com.hartwig.pipeline.Arguments;

public class ResourceFilesFactory {
    public static ResourceFiles buildResourceFiles(Arguments arguments) {
        if (arguments.customResourcesBucket().isPresent()) {
            return new RuntimeResourceFiles(arguments);
        }
        return arguments.refGenomeVersion() == RefGenomeVersion.HG19 ? new Hg19ResourceFiles() : new Hg38ResourceFiles();
    }

    public static ResourceFiles buildResourceFiles(RefGenomeVersion refGenomeVersion) {
        return refGenomeVersion == RefGenomeVersion.HG19 ? new Hg19ResourceFiles() : new Hg38ResourceFiles();
    }
}
