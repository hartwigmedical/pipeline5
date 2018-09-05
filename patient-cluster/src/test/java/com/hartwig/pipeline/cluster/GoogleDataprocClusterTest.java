package com.hartwig.pipeline.cluster;

import java.io.FileInputStream;
import java.io.IOException;

import com.google.api.services.dataproc.DataprocScopes;
import com.google.auth.oauth2.GoogleCredentials;
import com.hartwig.patient.Patient;
import com.hartwig.patient.Sample;

import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class GoogleDataprocClusterTest {

    @Test
    public void createdSimpleCluster() throws IOException {
        final GoogleCredentials credential =
                GoogleCredentials.fromStream(new FileInputStream(System.getProperty("user.dir") + "/bootstrap-key.json"))
                        .createScoped(DataprocScopes.all());
        PatientCluster victim = new GoogleDataprocCluster("hmf-pipeline-development",
                "europe-west4",
                Patient.of("", "GIAB12878", Sample.builder("", "").build()),
                credential);
        victim.start();
    }
}