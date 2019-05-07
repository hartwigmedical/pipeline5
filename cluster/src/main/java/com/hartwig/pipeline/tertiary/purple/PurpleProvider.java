package com.hartwig.pipeline.tertiary.purple;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.execution.vm.ComputeEngine;
import com.hartwig.pipeline.io.ResultsDirectory;
import com.hartwig.pipeline.tertiary.cobalt.Cobalt;

public class PurpleProvider {

    private final Arguments arguments;
    private final GoogleCredentials credentials;
    private final Storage storage;

    private PurpleProvider(final Arguments arguments, final GoogleCredentials credentials, final Storage storage) {
        this.arguments = arguments;
        this.credentials = credentials;
        this.storage = storage;
    }

    public static PurpleProvider from(final Arguments arguments, final GoogleCredentials credentials, final Storage storage) {
        return new PurpleProvider(arguments, credentials, storage);
    }

    public Purple get() throws Exception {
        return new Purple(arguments, ComputeEngine.from(arguments, credentials), storage, ResultsDirectory.defaultDirectory());
    }
}
