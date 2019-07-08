package com.hartwig.pipeline.metrics;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.execution.vm.ComputeEngine;

public class BamMetricsProvider {
    private final Arguments arguments;
    private final GoogleCredentials credentials;
    private final Storage storage;

    private BamMetricsProvider(Arguments arguments, GoogleCredentials credentials, Storage storage) {
        this.arguments = arguments;
        this.credentials = credentials;
        this.storage = storage;
    }

    public static BamMetricsProvider from(final Arguments arguments, final GoogleCredentials credentials, final Storage storage) {
        return new BamMetricsProvider(arguments, credentials, storage);
    }

    public BamMetrics get() throws Exception {
        ResultsDirectory resultsDirectory = ResultsDirectory.defaultDirectory();
        return new BamMetrics(arguments, ComputeEngine.from(arguments, credentials), storage, resultsDirectory);
    }
}
