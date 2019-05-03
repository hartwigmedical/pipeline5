package com.hartwig.pipeline.tertiary.cobalt;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.execution.vm.ComputeEngine;
import com.hartwig.pipeline.io.NamespacedResults;

public class CobaltProvider {

    private final Arguments arguments;
    private final GoogleCredentials credentials;
    private final Storage storage;

    private CobaltProvider(final Arguments arguments, final GoogleCredentials credentials, final Storage storage) {
        this.arguments = arguments;
        this.credentials = credentials;
        this.storage = storage;
    }

    public static CobaltProvider from(final Arguments arguments, final GoogleCredentials credentials, final Storage storage) {
        return new CobaltProvider(arguments, credentials, storage);
    }

    public Cobalt get() throws Exception {
        NamespacedResults namespacedResults = NamespacedResults.of(Cobalt.RESULTS_NAMESPACE);
        return new Cobalt(arguments, ComputeEngine.from(arguments, credentials), storage, namespacedResults);
    }
}
