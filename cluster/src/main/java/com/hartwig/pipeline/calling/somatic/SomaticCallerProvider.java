package com.hartwig.pipeline.calling.somatic;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.execution.vm.ComputeEngine;
import com.hartwig.pipeline.io.ResultsDirectory;

public class SomaticCallerProvider {

    private final Arguments arguments;
    private final GoogleCredentials credentials;
    private final Storage storage;

    private SomaticCallerProvider(final Arguments arguments, final GoogleCredentials credentials, final Storage storage) {
        this.arguments = arguments;
        this.credentials = credentials;
        this.storage = storage;
    }

    public static SomaticCallerProvider from(final Arguments arguments, final GoogleCredentials credentials, final Storage storage) {
        return new SomaticCallerProvider(arguments, credentials, storage);
    }

    public SomaticCaller get() throws Exception {
        return new SomaticCaller(arguments,
                ComputeEngine.from(arguments, credentials),
                storage,
                ResultsDirectory.defaultDirectory());
    }
}
