package com.hartwig.pipeline.bootstrap;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.FileInputStream;

import com.google.api.services.dataproc.DataprocScopes;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.hartwig.pipeline.cluster.GoogleDataprocCluster;
import com.hartwig.pipeline.cluster.GoogleStorageJarUpload;
import com.hartwig.pipeline.upload.LocalToGoogleStorage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class BootstrapTest {

    private static final String PATIENT = "CPCT12345678";
    private static final String BUCKET = "pipeline5-runtime";
    private static final String PROJECT_ID = "hmf-pipeline-development";
    private Storage storage;
    private Bootstrap victim;

    @Before
    public void setUp() throws Exception {
        final GoogleCredentials credentials =
                GoogleCredentials.fromStream(new FileInputStream(System.getProperty("user.dir") + "/bootstrap-key.json"))
                        .createScoped(DataprocScopes.all());
        storage = StorageOptions.newBuilder().setCredentials(credentials).setProjectId(PROJECT_ID).build().getService();
        victim = new Bootstrap(() -> new LocalToGoogleStorage(storage),
                () -> new GoogleDataprocCluster(credentials),
                new GoogleStorageJarUpload(storage),
                FileSystem.getLocal(new Configuration()));
        if (storage.get(BUCKET) == null) {
            storage.create(BucketInfo.of(BUCKET));
        }
    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void uploadsDataInPatientDirectory() {
        victim.run(Arguments.builder()
                .patientId(PATIENT)
                .version("local-SNAPSHOT")
                .jarLibDirectory("/Users/pwolfe/Code/pipeline2/system/target/")
                .patientDirectory(System.getProperty("user.dir") + "/src/test/resources/patients/cancerPanel")
                .build());
        assertThat(storage.get(BUCKET).list().iterateAll()).isNotEmpty();
    }
}