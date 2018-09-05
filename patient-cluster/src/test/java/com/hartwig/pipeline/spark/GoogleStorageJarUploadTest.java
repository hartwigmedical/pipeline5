package com.hartwig.pipeline.spark;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.FileInputStream;

import com.google.api.services.dataproc.DataprocScopes;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class GoogleStorageJarUploadTest {

    private Storage storage;
    private JarUpload victim;

    @Before
    public void setUp() throws Exception {
        final GoogleCredentials credentials =
                GoogleCredentials.fromStream(new FileInputStream(System.getProperty("user.dir") + "/bootstrap-key.json"))
                        .createScoped(DataprocScopes.all());
        storage = StorageOptions.newBuilder().setCredentials(credentials).setProjectId("hmf-pipeline-development").build().getService();
        victim = new GoogleStorageJarUpload(storage, "europe-west4", "/Users/pwolfe/Code/pipeline2/system/target/");
    }

    @Test
    public void createsJarBucketIfNotExists() throws Exception {
        victim.run(Version.of("local-SNAPSHOT"));
        assertThat(storage.get(GoogleStorageJarUpload.JAR_BUCKET).exists()).isTrue();
    }

    @Test
    public void uploadsJarWhenVersionIsNotPresent() throws Exception {
        JarLocation location = victim.run(Version.of("local-SNAPSHOT"));
        assertThat(storage.get(GoogleStorageJarUpload.JAR_BUCKET).get(location.uri())).isNotNull();
    }

    @Test
    public void doesNothingWhenJarVersionAlreadyExists() throws Exception {
        JarLocation location = victim.run(Version.of("local-SNAPSHOT"));
        assertThat(storage.get(GoogleStorageJarUpload.JAR_BUCKET).get(location.uri())).isNotNull();
    }
}