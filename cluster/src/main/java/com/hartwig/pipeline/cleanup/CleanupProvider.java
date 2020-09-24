package com.hartwig.pipeline.cleanup;

import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;

public class CleanupProvider {

    private final Arguments arguments;
    private final Storage storage;

    private CleanupProvider(final Arguments arguments, final Storage storage) {
        this.arguments = arguments;
        this.storage = storage;
    }

    public static CleanupProvider from(final Arguments arguments, final Storage storage) {
        return new CleanupProvider(arguments, storage);
    }

    public Cleanup get() {
        return new Cleanup(arguments, storage);
    }
}
