package com.hartwig.pipeline.flagstat;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.execution.vm.ComputeEngine;
import com.hartwig.pipeline.io.ResultsDirectory;

public class FlagstatProvider {
    private final Arguments arguments;
    private final GoogleCredentials credentials;
    private final Storage storage;

    private FlagstatProvider(Arguments arguments, GoogleCredentials credentials, Storage storage) {
        this.arguments = arguments;
        this.credentials = credentials;
        this.storage = storage;
    }

    public static FlagstatProvider from(final Arguments arguments, final GoogleCredentials credentials, final Storage storage) {
        return new FlagstatProvider(arguments, credentials, storage);
    }

    public Flagstat get() throws Exception {
        ResultsDirectory resultsDirectory = ResultsDirectory.defaultDirectory();
        return new Flagstat(arguments, ComputeEngine.from(arguments, credentials), storage, resultsDirectory);
    }
}
