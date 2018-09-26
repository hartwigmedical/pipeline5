package com.hartwig.pipeline.upload;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hartwig.pipeline.bootstrap.Arguments;

import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.HttpUrlConnectorProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SBPRestApi {

    private final static ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final static Logger LOGGER = LoggerFactory.getLogger(SBPRestApi.class);
    private final WebTarget target;

    private SBPRestApi(final WebTarget target) {
        this.target = target;
    }

    String getFastQ(int sampleId) {
        //  return "[]";
        /*LOGGER.info("Connecting to SBP at [{}] for sample id [{}]", target.getUri(), sampleId);
        Response response = target.path("hmf").path("v1").path("fastq").queryParam("sample_id", sampleId).request().buildGet().invoke();
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return response.readEntity(String.class);
        }
        throw new RuntimeException(String.format("Received an error status of [%s] from SBP Api at [%s]",
                response.getStatus(),
                target.getUri()));*/
        return "[\n" + "  {\n" + "    \"qc_pass\": true,\n" + "    \"q30\": 55.9669,\n" + "    \"sample_id\": 64,\n" + "    \"id\": 91,\n"
                + "    \"name_r1\": \"CPCT12345678R_HJJLGCCXX_S1_L001_R1_001.fastq.gz\",\n"
                + "    \"name_r2\": \"CPCT12345678R_HJJLGCCXX_S1_L001_R2_001.fastq.gz\",\n" + "    \"lane_id\": 58,\n"
                + "    \"bucket\": \"hmf-fastq-storage\",\n" + "    \"yld\": 59702984,\n" + "    \"hash_r2\": null,\n"
                + "    \"hash_r1\": null,\n" + "    \"size_r2\": null,\n" + "    \"size_r1\": null\n" + "  }]";
    }

    void patchBam(int sampleId, BamMetadata metadata) {
        try {
            String json = OBJECT_MAPPER.writeValueAsString(metadata);
            LOGGER.info("Patching sample [{}] with [{}]", sampleId, json);
            Response response = target.path("hmf")
                    .path("v1")
                    .path("samples")
                    .path(String.valueOf(sampleId))
                    .request()
                    .build("PATCH", Entity.entity(json, MediaType.APPLICATION_JSON_TYPE))
                    .invoke();
            LOGGER.info("Patching complete with response [{}]", response.getStatus());

        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public static SBPRestApi newInstance(Arguments arguments) {
        ClientConfig config = new ClientConfig();
        config.property(HttpUrlConnectorProvider.SET_METHOD_WORKAROUND, true);
        return new SBPRestApi(ClientBuilder.newBuilder().withConfig(config).build().target(arguments.sbpApiUrl()));
    }
}
