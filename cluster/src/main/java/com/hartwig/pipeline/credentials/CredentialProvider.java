package com.hartwig.pipeline.credentials;

import java.io.FileInputStream;
import java.io.IOException;

import com.google.api.services.dataproc.v1beta2.DataprocScopes;
import com.google.auth.oauth2.GoogleCredentials;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.io.GSUtil;

public class CredentialProvider {

    private final Arguments arguments;

    private CredentialProvider(final Arguments arguments) {
        this.arguments = arguments;
    }

    public GoogleCredentials get() throws IOException, InterruptedException {
        GoogleCredentials credentials =
                GoogleCredentials.fromStream(new FileInputStream(arguments.privateKeyPath())).createScoped(DataprocScopes.all());
        GSUtil.configure(false, 4);
        GSUtil.auth(arguments.cloudSdkPath(), arguments.privateKeyPath());
        return credentials;
    }

    public static CredentialProvider from(final Arguments arguments) {
        return new CredentialProvider(arguments);
    }
}
