package com.hartwig.pipeline.cleanup;

import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.dataproc.v1beta2.Dataproc;
import com.google.auth.Credentials;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;

public class CleanupProvider {

    private final Credentials credentials;
    private final Arguments arguments;
    private final Storage storage;

    private CleanupProvider(final Credentials credentials, final Arguments arguments, final Storage storage) {
        this.credentials = credentials;
        this.arguments = arguments;
        this.storage = storage;
    }

    public static CleanupProvider from(final Credentials credentials, final Arguments arguments, final Storage storage) {
        return new CleanupProvider(credentials, arguments, storage);
    }

    public Cleanup get() {
        return new Cleanup(storage,
                arguments,
                new Dataproc.Builder(new NetHttpTransport(),
                        JacksonFactory.getDefaultInstance(),
                        new HttpCredentialsAdapter(credentials)).setApplicationName("cleanup").build());
    }
}
