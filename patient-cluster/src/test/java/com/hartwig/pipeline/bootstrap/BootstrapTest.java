package com.hartwig.pipeline.bootstrap;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.FileInputStream;

import com.google.api.services.dataproc.DataprocScopes;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.hartwig.pipeline.cluster.GoogleDataprocCluster;
import com.hartwig.pipeline.spark.GoogleStorageJarUpload;
import com.hartwig.pipeline.spark.Version;
import com.hartwig.pipeline.upload.LocalToGoogleStorage;

import org.apache.commons.cli.DefaultParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class BootstrapTest {

    private static final String PATIENT = "CPCT12345678";
    private static final String BUCKET = "pipeline5-patients";
    private static final String PROJECT_ID = "hmf-pipeline-development";
    private static final String REGION = "europe-west4";
    private Storage storage;
    private Bootstrap victim;

    @Before
    public void setUp() throws Exception {
        final GoogleCredentials credentials =
                GoogleCredentials.fromStream(new FileInputStream(System.getProperty("user.dir") + "/bootstrap-key.json"))
                        .createScoped(DataprocScopes.all());
        storage = StorageOptions.newBuilder().setCredentials(credentials).setProjectId(PROJECT_ID).build().getService();
        victim = new Bootstrap(patient -> new LocalToGoogleStorage(storage, BUCKET, patient),
                patient -> new GoogleDataprocCluster(PROJECT_ID, REGION, patient, credentials),
                new GoogleStorageJarUpload(storage, "europe-west4", "/Users/pwolfe/Code/pipeline2/system/target/"),
                Version.of("local-SNAPSHOT"),
                new DefaultParser(),
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
        victim.run(new String[] { arg(BootstrapOptions.PATIENT_FLAG), PATIENT, arg(BootstrapOptions.PATIENT_DIRECTORY_FLAG),
                System.getProperty("user.dir") + "/src/test/resources/patients/cancerPanel" });
        assertThat(storage.get(BUCKET).list().iterateAll()).isNotEmpty();
    }

    private static String arg(final String skipUploadFlag) {
        return "-" + skipUploadFlag;
    }
}