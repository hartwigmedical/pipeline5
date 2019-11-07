package com.hartwig.pipeline.storage;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.hartwig.pipeline.CommonArguments;

public class StorageProvider {

    private final CommonArguments arguments;
    private final GoogleCredentials credentials;

    private StorageProvider(final CommonArguments arguments, final GoogleCredentials credentials) {
        this.arguments = arguments;
        this.credentials = credentials;
    }

    public Storage get() {
        StorageOptions.Builder builder = StorageOptions.newBuilder();
        return builder.setCredentials(credentials)
                .setProjectId(arguments.project())
                .setTransportOptions(HttpTransportOptions.newBuilder()
                        .setConnectTimeout(0)
                        .setReadTimeout(0)
                        .build())
                .build()
                .getService();
    }

    public static StorageProvider from(CommonArguments arguments, GoogleCredentials credentials) {
        return new StorageProvider(arguments, credentials);
    }
}
