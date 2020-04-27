package com.hartwig.pipeline.credentials;

import java.io.FileInputStream;
import java.io.IOException;

import com.google.api.services.compute.ComputeScopes;
import com.google.auth.oauth2.GoogleCredentials;
import com.hartwig.pipeline.CommonArguments;
import com.hartwig.pipeline.storage.GSUtil;

public class CredentialProvider {

    private final CommonArguments arguments;

    private CredentialProvider(final CommonArguments arguments) {
        this.arguments = arguments;
    }

    public GoogleCredentials get() throws IOException, InterruptedException {
        GoogleCredentials credentials =
                arguments.privateKeyPath().isPresent() ? GoogleCredentials.fromStream(new FileInputStream(arguments.privateKeyPath().get()))
                        .createScoped(ComputeScopes.all()) : GoogleCredentials.getApplicationDefault();
        ;
        GSUtil.configure(false, 4);
        authorize(arguments);
        return credentials;
    }

    public static void authorize(final CommonArguments arguments) throws IOException, InterruptedException {
        if (arguments.privateKeyPath().isPresent()) {
            GSUtil.auth(arguments.cloudSdkPath(), arguments.privateKeyPath().get());
        }
    }

    public static CredentialProvider from(final CommonArguments arguments) {
        return new CredentialProvider(arguments);
    }
}