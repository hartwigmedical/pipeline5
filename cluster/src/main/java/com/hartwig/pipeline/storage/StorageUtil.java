package com.hartwig.pipeline.storage;

import com.hartwig.pipeline.input.RunMetadata;
import com.hartwig.computeengine.storage.RunIdentifier;
import com.hartwig.pipeline.Arguments;

public final class StorageUtil {
    private StorageUtil() {
    }

    public static RunIdentifier runIdentifierFromArguments(RunMetadata metadata, Arguments arguments) {
        if (arguments.runTag().isPresent()) {
            return RunIdentifier.from(metadata.runName(), arguments.runTag().get());
        } else if (arguments.sbpApiRunId().isPresent()) {
            return RunIdentifier.from(metadata.runName(), arguments.sbpApiRunId().get());
        }
        return RunIdentifier.from(metadata.runName());
    }
}
