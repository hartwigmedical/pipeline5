package com.hartwig.pipeline.tertiary.healthcheck;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.execution.vm.ComputeEngine;
import com.hartwig.pipeline.io.ResultsDirectory;

public class HealthCheckerProvider {
    private final Arguments arguments;
    private final GoogleCredentials credentials;
    private final Storage storage;

    private HealthCheckerProvider(final Arguments arguments, final GoogleCredentials credentials, final Storage storage) {
        this.arguments = arguments;
        this.credentials = credentials;
        this.storage = storage;
    }

    public static HealthCheckerProvider from(final Arguments arguments, final GoogleCredentials credentials, final Storage storage) {
        return new HealthCheckerProvider(arguments, credentials, storage);
    }

    public HealthChecker get() throws Exception {
        ResultsDirectory resultsDirectory = ResultsDirectory.defaultDirectory();
        return new HealthChecker(arguments, ComputeEngine.from(arguments, credentials), storage, resultsDirectory);
    }
}
