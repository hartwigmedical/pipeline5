package com.hartwig.pipeline.tertiary.amber;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.execution.vm.ComputeEngine;

public class AmberProvider {

    private final Arguments arguments;
    private final GoogleCredentials credentials;
    private final Storage storage;

    private AmberProvider(final Arguments arguments, final GoogleCredentials credentials, final Storage storage) {
        this.arguments = arguments;
        this.credentials = credentials;
        this.storage = storage;
    }

    public static AmberProvider from(final Arguments arguments, final GoogleCredentials credentials, final Storage storage) {
        return new AmberProvider(arguments, credentials, storage);
    }

    public Amber get() throws Exception {
        ResultsDirectory resultsDirectory = ResultsDirectory.defaultDirectory();
        return new Amber(arguments, ComputeEngine.from(arguments, credentials), storage, resultsDirectory);
    }
}
