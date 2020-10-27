package com.hartwig.pipeline.sbpapi;

import static java.lang.String.format;
import static java.lang.String.valueOf;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hartwig.pipeline.jackson.ObjectMappers;

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
        Response response = runs().path(valueOf(id)).request().buildGet().invoke();
        return returnOrThrow(response);
    }

    private String returnOrThrow(final Response response) {
        if (response.getStatusInfo().getFamily() == Response.Status.Family.SUCCESSFUL) {
            return response.readEntity(String.class);
        }
        throw error(response);
    }

    private WebTarget runs() {
        return api().path(RUNS);
    }

    public String getSample(int sampleId) {
        Response response = sample().path(valueOf(sampleId)).request().buildGet().invoke();
        return returnOrThrow(response);
    }

    public String getDataset(final String biopsyName) {
        return returnOrThrow(api().path("datasets").queryParam("biopsy", biopsyName).queryParam("output", "condensed").request().get());
    }

    public WebTarget sample() {
        return api().path(SAMPLES);
    }

    public String getSample(String setId) {
        Response response = sample().queryParam("set_id", setId).request().buildGet().invoke();
        return returnOrThrow(response);
    }

    public String getSamplesByBiopsy(final String biopsyName) {
        try {
            Map<String, String> biopsy = ObjectMappers.get().<List<Map<String, String>>>readValue(returnOrThrow(api().path("biopsies")
                    .queryParam("name", biopsyName)
                    .request()
                    .get()), new TypeReference<List<Map<String, String>>>() {
            }).stream()
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException(String.format("No biopsies were found for name [%s].", biopsyName)));
            return returnOrThrow(sample().queryParam("biopsy_id", biopsy.get("id")).request().buildGet().invoke());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void updateRunStatus(String runID, String status, String gcpBucket) {
        LOGGER.info("Patching {} id [{}] with status [{}]", SbpRestApi.RUNS, runID, status);
        returnOrThrow(api().path(RUNS).path(runID).request().build("PATCH", jsonEntity(SbpRunStatusUpdate.of(status, gcpBucket))).invoke());
    }

    public AddFileApiResponse postFile(final SbpFileMetadata metaData) {
        try {
            return ObjectMappers.get()
                    .readValue(returnOrThrow(api().path(FILES).request().post(jsonEntity(metaData))), AddFileApiResponse.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void linkFileToSample(final int id, final String barcode) {
        try {
            SbpSample sample = ObjectMappers.get().<List<SbpSample>>readValue(returnOrThrow(api().path(SAMPLES)
                    .queryParam("barcode", barcode)
                    .request()
                    .get()), new TypeReference<List<SbpSample>>() {
            }).stream().findFirst().orElseThrow();
            Map<String, Integer> link = new HashMap<>();
            link.put("sample_id", sample.id());
            returnOrThrow(api().path(FILES).path(valueOf(id)).path("sample").request().post(jsonEntity(link)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> Entity<String> jsonEntity(T payload) {
        try {
            return Entity.entity(OBJECT_MAPPER.writeValueAsString(payload), MediaType.APPLICATION_JSON_TYPE);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public void patchFile(final int id, final String key, final String value) {
        Map<String, String> patchedFields = new HashMap<>();
        patchedFields.put(key, value);
        returnOrThrow(api().path(FILES).path(valueOf(id)).request().build("PATCH", jsonEntity(patchedFields)).invoke());
    }

    private WebTarget api() {
        return target.path("hmf").path("v1");
    }

    private String getBySampleId(final int sampleId, final WebTarget path) {
        Response response = path.queryParam("sample_id", sampleId).request().buildGet().invoke();
        return returnOrThrow(response);
    }

    private RuntimeException error(final Response response) {
        return new RuntimeException(format("Received an error status result [%s] of SBP Api at [%s] with message [%s]",
                response.getStatus(),
                target.getUri(),
                response.readEntity(String.class)));
    }

    public static SbpRestApi newInstance(final String url) {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.property(HttpUrlConnectorProvider.SET_METHOD_WORKAROUND, true);
        return new SbpRestApi(ClientBuilder.newBuilder().withConfig(clientConfig).build().target(url));
    }
}
