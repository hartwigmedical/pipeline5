package com.hartwig.pipeline.io.sbp;

import org.junit.Test;

public class SBPRestApiTest {

    @Test
    public void findsPatientFilesByUsingSBPAPI() throws Exception {
   /*     SslConfigurator sslConfigurator = SslConfigurator.newInstance()
                .trustStoreFile("/Users/pwolfe/Code/pipeline2/patient-cluster/api.jks")
                .keyStoreFile("/Users/pwolfe/Code/pipeline2/patient-cluster/api.jks");
        Client apiClient = ClientBuilder.newBuilder().sslContext(sslConfigurator.createSSLContext()).build();
        SBPSampleReader victim = new SBPSampleReader(apiClient);
        victim.run(Patient.of("", "CPCT12345678", Sample.builder("", "").build()),
                Arguments.builder()
                        .patientId("CPCT12345678")
                        .version("local-SNAPSHOT")
                        .runtimeBucket("bucket")
                        .project("project")
                        .region("region")
                        .privateKeyPath("path")
                        .sbpApiUrl("https://api.hartwigmedicalfoundation.nl")
                        .jarDirectory("/Users/pwolfe/Code/pipeline2/system/target/")
                        .sampleDirectory(System.getProperty("user.dir") + "/src/test/resources/patients/cancerPanel")
                        .build());*/
    }
}