package com.hartwig.pipeline.calling.structural;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.execution.vm.ComputeEngine;
import com.hartwig.pipeline.io.ResultsDirectory;

public class StructuralCallerProvider {
    private final Arguments arguments;
    private final GoogleCredentials credentials;
    private final Storage storage;

    private StructuralCallerProvider(Arguments arguments, GoogleCredentials credentials, Storage storage) {
        this.arguments = arguments;
        this.credentials = credentials;
        this.storage = storage;
    }

    public static StructuralCallerProvider from(final Arguments arguments, final GoogleCredentials credentials, final Storage storage){
        return new StructuralCallerProvider(arguments, credentials, storage);
    }

    public StructuralCaller get() throws Exception {
        return new StructuralCaller(arguments, ComputeEngine.from(arguments, credentials), storage,
                ResultsDirectory.defaultDirectory());
    }
}
