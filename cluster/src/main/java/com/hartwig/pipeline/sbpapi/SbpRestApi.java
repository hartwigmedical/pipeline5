package com.hartwig.pipeline.sbpapi;

import static java.lang.String.format;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

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
    private static final String FASTQ = "fastq";
    private final WebTarget target;

    private SbpRestApi(final WebTarget target) {
        this.target = target;
    }

    public String getInis() {
        return returnOrThrow(api().path(INIS).request().buildGet().invoke());
    }

    public String getFastQ(int sampleId) {
        return getBySampleId(sampleId, api().path(FASTQ));
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

    public String getSampleByName(String sampleName) {
        Response response = sample().queryParam("name", sampleName).request().buildGet().invoke();
        return returnOrThrow(response);
    }

    public void updateRunStatus(String runID, String status, String gcpBucket) {
        try {
            String json = OBJECT_MAPPER.writeValueAsString(SbpRunStatusUpdate.of(status, gcpBucket));
            patchRun(runID, status, json);

        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public void postFile(final SbpFileMetadata metaData) {
        try {
            post(api().path(FILES), OBJECT_MAPPER.writeValueAsString(metaData));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public int postSample(final SbpSample sample) {
        try {
            post(api().path(SAMPLES), OBJECT_MAPPER.writeValueAsString(sample));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return 0;
    }

    public void postFastq(final SbpFastQ fastQ) {
        try {
            post(api().path(FASTQ), OBJECT_MAPPER.writeValueAsString(fastQ));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private void post(final WebTarget path, final String json) throws JsonProcessingException {
        Response response = path.request().buildPost(Entity.entity(json, MediaType.APPLICATION_JSON_TYPE)).invoke();
        if (response.getStatus() != Response.Status.CREATED.getStatusCode()) {
            LOGGER.error("Failed to POST file data: {}", response.readEntity(String.class));
            throw error(response);
        }
    }

    private void patchRun(final String sampleID, final String status, final String json) {
        LOGGER.info("Patching {} id [{}] with status [{}]", SbpRestApi.RUNS, sampleID, status);
        Response response =
                api().path(RUNS).path(sampleID).request().build("PATCH", Entity.entity(json, MediaType.APPLICATION_JSON_TYPE)).invoke();
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

    public static SbpRestApi newInstance(final String url) {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.property(HttpUrlConnectorProvider.SET_METHOD_WORKAROUND, true);
        return new SbpRestApi(ClientBuilder.newBuilder().withConfig(clientConfig).build().target(url));
    }
}
