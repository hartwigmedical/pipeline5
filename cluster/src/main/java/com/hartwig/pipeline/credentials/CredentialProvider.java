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
                GoogleCredentials.fromStream(new FileInputStream(arguments.privateKeyPath())).createScoped(ComputeScopes.all());
        GSUtil.configure(false, 4);
        GSUtil.auth(arguments.cloudSdkPath(), arguments.privateKeyPath());
        return credentials;
    }

    public static CredentialProvider from(final CommonArguments arguments) {
        return new CredentialProvider(arguments);
    }
}