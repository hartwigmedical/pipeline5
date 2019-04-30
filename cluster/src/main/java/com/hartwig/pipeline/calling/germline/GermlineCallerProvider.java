package com.hartwig.pipeline.calling.germline;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.execution.vm.ComputeEngine;

public class GermlineCallerProvider {

    private final Arguments arguments;
    private final GoogleCredentials credentials;
    private final Storage storage;

    private GermlineCallerProvider(final Arguments arguments, final GoogleCredentials credentials, final Storage storage) {
        this.arguments = arguments;
        this.credentials = credentials;
        this.storage = storage;
    }

    public static GermlineCallerProvider from(final GoogleCredentials credentials, final Storage storage, final Arguments arguments) {
        return new GermlineCallerProvider(arguments, credentials, storage);
    }

    public GermlineCaller get() throws Exception {
        return new GermlineCaller(arguments, ComputeEngine.from(arguments, credentials), storage);
    }
}
