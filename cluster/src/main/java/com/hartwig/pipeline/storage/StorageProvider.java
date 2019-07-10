package com.hartwig.pipeline.storage;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.hartwig.pipeline.Arguments;

public class StorageProvider {

    private final Arguments arguments;
    private final GoogleCredentials credentials;

    private StorageProvider(final Arguments arguments, final GoogleCredentials credentials) {
        this.arguments = arguments;
        this.credentials = credentials;
    }

    public Storage get() {
        StorageOptions.Builder builder = StorageOptions.newBuilder();
        return builder.setCredentials(credentials)
                .setProjectId(arguments.project())
                .setTransportOptions(HttpTransportOptions.newBuilder()
                        .setConnectTimeout(Integer.MAX_VALUE)
                        .setReadTimeout(Integer.MAX_VALUE)
                        .build())
                .build()
                .getService();
    }

    public static StorageProvider from(Arguments arguments, GoogleCredentials credentials) {
        return new StorageProvider(arguments, credentials);
    }
}
