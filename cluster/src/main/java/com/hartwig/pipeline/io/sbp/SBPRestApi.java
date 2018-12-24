package com.hartwig.pipeline.io.sbp;

import java.util.Map;

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
import org.jetbrains.annotations.NotNull;
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
        LOGGER.info("Connecting to SBP API at [{}] for sample id [{}]", target.getUri(), sampleId);
        Response response = target.path("hmf").path("v1").path("fastq").queryParam("sample_id", sampleId).request().buildGet().invoke();
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return response.readEntity(String.class);
        }
        throw error(response);
    }

    @NotNull
    private RuntimeException error(final Response response) {
        return new RuntimeException(String.format("Received an error status of [%s] from SBP Api at [%s]",
                response.getStatus(),
                target.getUri()));
    }

    String getBarcode(int sampleId) {
        Response response = samplesApi().path(String.valueOf(sampleId)).request().buildGet().invoke();
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return response.readEntity(Map.class).get("barcode").toString();
        }
        throw error(response);
    }

    private WebTarget samplesApi() {
        return target.path("hmf").path("v1").path("samples");
    }

    void patchBam(int sampleId, BamMetadata metadata) {
        try {
            String json = OBJECT_MAPPER.writeValueAsString(metadata);
            LOGGER.info("Patching sample [{}] with [{}]", sampleId, json);
            Response response = samplesApi()
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
