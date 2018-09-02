package com.hartwig.pipeline.cluster;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

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
    private static final String BUCKET = "pipeline5-test-patients";
    private Storage storage;
    private Bootstrap victim;

    @Before
    public void setUp() throws Exception {
        storage = StorageOptions.newBuilder().setProjectId("hmf-pipeline-development").build().getService();
        victim = new Bootstrap(BUCKET, storage, new DefaultParser(), FileSystem.getLocal(new Configuration()));
        if (storage.get(BUCKET) == null) {
            storage.create(BucketInfo.of(BUCKET));
        }
    }

    @After
    public void tearDown() throws Exception {
        //    storage.delete(BUCKET);
    }

    @Test
    public void noBucketCreatedWhenSkipCommandLineEnabled() {
        victim.run(new String[] { arg(BootstrapOptions.SKIP_UPLOAD_FLAG), arg(BootstrapOptions.PATIENT_FLAG) });
        assertThat(storage.get(BUCKET).list().iterateAll()).isEmpty();
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