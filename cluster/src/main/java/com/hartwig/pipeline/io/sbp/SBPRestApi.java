package com.hartwig.pipeline.io.sbp;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.metadata.SbpStatusUpdate;

import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.HttpUrlConnectorProvider;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SBPRestApi {

    private final static ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final static Logger LOGGER = LoggerFactory.getLogger(SBPRestApi.class);
    public static final String SAMPLES = "samples";
    public static final String RUNS = "runs";
    private final WebTarget target;

    private SBPRestApi(final WebTarget target) {
        this.target = target;
    }

    String getFastQ(int sampleId) {
        return getBySampleId(sampleId, api().path("fastq"));
    }

    public String getSet(int sampleId) {
        return getBySampleId(sampleId, api().path("sets"));
    }

    public String getRun(int setId) {
        Response response = api().path(RUNS).path(String.valueOf(setId)).request().buildGet().invoke();
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return response.readEntity(String.class);
        }
        throw error(response);
    }

    public String getSample(int sampleId) {
        Response response = api().path(SAMPLES).path(String.valueOf(sampleId)).request().buildGet().invoke();
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return response.readEntity(String.class);
        }
        throw error(response);
    }

    public void updateStatus(String entityType, String entityId, String status) {
        try {
            String json = OBJECT_MAPPER.writeValueAsString(SbpStatusUpdate.of(status));
            LOGGER.info("Patching entity type [{}] id [{}] with status [{}]", entityType, entityId, status);
            Response response = api().path(entityType)
                    .path(entityId)
                    .request()
                    .build("PATCH", Entity.entity(json, MediaType.APPLICATION_JSON_TYPE))
                    .invoke();
            LOGGER.info("Patching complete with response [{}]", response.getStatus());

        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private WebTarget api() {
        return target.path("hmf").path("v1");
    }

    private String getBySampleId(final int sampleId, final WebTarget path) {
        Response response = path.queryParam("sample_id", sampleId).request().buildGet().invoke();
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return response.readEntity(String.class);
        }
        throw error(response);
    }

    @NotNull
    private RuntimeException error(final Response response) {
        return new RuntimeException(String.format("Received an error status defaultDirectory [%s] of SBP Api at [%s]",
                response.getStatus(),
                target.getUri()));
    }

    public static SBPRestApi newInstance(Arguments arguments) {
        ClientConfig config = new ClientConfig();
        config.property(HttpUrlConnectorProvider.SET_METHOD_WORKAROUND, true);
        return new SBPRestApi(ClientBuilder.newBuilder().withConfig(config).build().target(arguments.sbpApiUrl()));
    }
}
