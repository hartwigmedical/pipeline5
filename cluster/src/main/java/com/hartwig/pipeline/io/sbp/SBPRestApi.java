package com.hartwig.pipeline.io.sbp;

import static java.lang.String.format;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.metadata.SbpRunStatusUpdate;
import com.hartwig.pipeline.metadata.SbpSampleStatusUpdate;

import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.HttpUrlConnectorProvider;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SBPRestApi {

    private final static ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final static Logger LOGGER = LoggerFactory.getLogger(SBPRestApi.class);
    private static final String SAMPLES = "samples";
    private static final String RUNS = "runs";
    private static final String FILES = "files";
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

    public String getSample(String setId) {
        Response response = api().path(SAMPLES).queryParam("set_id", setId).request().buildGet().invoke();
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return response.readEntity(String.class);
        }
        throw error(response);
    }

    public void updateSampleStatus(String sampleID, String status) {
        try {
            String json = OBJECT_MAPPER.writeValueAsString(SbpSampleStatusUpdate.of(status));
            patch(sampleID, status, json, SAMPLES);

        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public void updateRunStatus(String runID, String status, String sbpBucket) {
        try {
            String json = OBJECT_MAPPER.writeValueAsString(SbpRunStatusUpdate.of(status, sbpBucket));
            patch(runID, status, json, RUNS);

        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    void postFile(final SbpFileMetadata metaData) {
        LOGGER.debug("Posting file [{}]", format("%s/%s(md5:%s)", metaData.directory(), metaData.filename(), metaData.hash()));
        try {
            String json = OBJECT_MAPPER.writeValueAsString(metaData);
            LOGGER.debug("Request JSON: {}", json);
            Response response = api().path(FILES).request().buildPost(Entity.entity(json, MediaType.APPLICATION_JSON_TYPE)).invoke();
            if (response.getStatus() != Response.Status.CREATED.getStatusCode()) {
                LOGGER.info("Failed to POST file data: {}", response.readEntity(String.class));
                throw error(response);
            }
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private void patch(final String sampleID, final String status, final String json, final String entityType) {
        LOGGER.info("Patching sample id [{}] with status [{}]", sampleID, status);
        Response response = api().path(entityType)
                .path(sampleID)
                .request()
                .build("PATCH", Entity.entity(json, MediaType.APPLICATION_JSON_TYPE))
                .invoke();
        LOGGER.info("Patching complete with response [{}]", response.getStatus());
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
        return new RuntimeException(format("Received an error status result [%s] of SBP Api at [%s]",
                response.getStatus(),
                target.getUri()));
    }

    public static SBPRestApi newInstance(Arguments arguments) {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.property(HttpUrlConnectorProvider.SET_METHOD_WORKAROUND, true);
        return new SBPRestApi(ClientBuilder.newBuilder().withConfig(clientConfig).build().target(arguments.sbpApiUrl()));
    }
}
