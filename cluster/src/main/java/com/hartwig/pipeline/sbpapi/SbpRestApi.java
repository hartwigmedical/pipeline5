package com.hartwig.pipeline.sbpapi;

import static java.lang.String.format;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hartwig.pipeline.Arguments;

import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.HttpUrlConnectorProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SbpRestApi {

    private final static ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final static Logger LOGGER = LoggerFactory.getLogger(SbpRestApi.class);
    private static final String SAMPLES = "samples";
    private static final String RUNS = "runs";
    private static final String FILES = "files";
    private static final String INIS = "inis";
    private final WebTarget target;

    private SbpRestApi(final WebTarget target) {
        this.target = target;
    }

    public String getInis() {
        return returnOrThrow(api().path(INIS).request().buildGet().invoke());
    }

    public String getFastQ(int sampleId) {
        return getBySampleId(sampleId, api().path("fastq"));
    }

    public String getSet(int sampleId) {
        return getBySampleId(sampleId, api().path("sets"));
    }

    public String getRun(int id) {
        Response response = runs().path(String.valueOf(id)).request().buildGet().invoke();
        return returnOrThrow(response);
    }

    private String returnOrThrow(final Response response) {
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return response.readEntity(String.class);
        }
        throw error(response);
    }

    private WebTarget runs() {
        return api().path(RUNS);
    }

    public String getSample(int sampleId) {
        Response response = sample().path(String.valueOf(sampleId)).request().buildGet().invoke();
        return returnOrThrow(response);
    }

    public WebTarget sample() {
        return api().path(SAMPLES);
    }

    public String getSample(String setId) {
        Response response = sample().queryParam("set_id", setId).request().buildGet().invoke();
        return returnOrThrow(response);
    }

    public void updateRunStatus(String runID, String status, String sbpBucket) {
        try {
            String json = OBJECT_MAPPER.writeValueAsString(SbpRunStatusUpdate.of(status, sbpBucket));
            patchRun(runID, status, json);

        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public void postFile(final SbpFileMetadata metaData) {
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

    private void patchRun(final String sampleID, final String status, final String json) {
        LOGGER.info("Patching {} id [{}] with status [{}]", SbpRestApi.RUNS, sampleID, status);
        Response response = api().path(RUNS)
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
        return returnOrThrow(response);
    }

    private RuntimeException error(final Response response) {
        return new RuntimeException(format("Received an error status result [%s] of SBP Api at [%s]",
                response.getStatus(),
                target.getUri()));
    }

    public static SbpRestApi newInstance(Arguments arguments) {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.property(HttpUrlConnectorProvider.SET_METHOD_WORKAROUND, true);
        return new SbpRestApi(ClientBuilder.newBuilder().withConfig(clientConfig).build().target(arguments.sbpApiUrl()));
    }
}
