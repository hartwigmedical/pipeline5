package com.hartwig.pipeline.credentials;

import com.google.auth.oauth2.GoogleCredentials;
import com.hartwig.pipeline.storage.GSUtil;

import javax.annotation.Nullable;
import java.io.FileInputStream;
import java.io.IOException;

public class CredentialProvider {

    private final String cloudSdkPath;
    @Nullable
    private final String privateKeyPath;

    private CredentialProvider(String cloudSdkPath, @Nullable String privateKeyPath) {
        this.cloudSdkPath = cloudSdkPath;
        this.privateKeyPath = privateKeyPath;
    }

    public GoogleCredentials get() throws IOException, InterruptedException {
        GoogleCredentials credentials = privateKeyPath != null ?
                GoogleCredentials.fromStream(new FileInputStream(privateKeyPath)).createScoped("https://www.googleapis.com/auth/cloud-platform") :
                GoogleCredentials.getApplicationDefault();
        GSUtil.configure(true, 4);
        if (privateKeyPath != null) {
            authorize(cloudSdkPath, privateKeyPath);
        }
        return credentials;
    }

    public static void authorize(String cloudSdkPath, String privateKeyPath) throws IOException, InterruptedException {
        GSUtil.auth(cloudSdkPath, privateKeyPath);
    }


    public static CredentialProvider from(String cloudSdkPath) {
        return new CredentialProvider(cloudSdkPath, null);
    }

    public static CredentialProvider from(String cloudSdkPath, @Nullable String privateKeyPath) {
        return new CredentialProvider(cloudSdkPath, privateKeyPath);
    }
}