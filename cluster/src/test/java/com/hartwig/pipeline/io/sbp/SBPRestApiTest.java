package com.hartwig.pipeline.io.sbp;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;

import org.junit.Test;

public class SBPRestApiTest {

    @Test
    public void findsPatientFilesByUsingSBPAPI() throws Exception {
       System.setProperty("javax.net.ssl.keyStore", "/Users/pwolfe/Code/pipeline5/cluster/certs/api.jks");
        System.setProperty("javax.net.ssl.keyStorePassword", "changeit");
      /*  System.setProperty("javax.net.ssl.trustStore", "/Users/pwolfe/Code/pipeline5/cluster/certs/apinew.jks");
        System.setProperty("javax.net.ssl.trustStorePassword", "changeit");*/

       // KeyStore keyStore = KeyStore.getInstance("JKS", "SUN");
      //  keyStore.load(new FileInputStream("/Users/pwolfe/Code/pipeline5/cluster/certs/api.jks"), "changeit".toCharArray());
        Client apiClient = ClientBuilder.newBuilder().build();
        SBPRestApi restApi = new SBPRestApi(apiClient.target("https://api.acc.hartwigmedicalfoundation.nl/"));
        System.out.println(restApi.getSample(4));
    }
}