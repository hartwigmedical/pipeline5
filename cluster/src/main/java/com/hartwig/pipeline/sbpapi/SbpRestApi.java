package com.hartwig.pipeline.sbpapi;

import static java.lang.String.format;
import static java.lang.String.valueOf;

import java.io.IOException;
import java.util.Collection;
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

    private final static ObjectMapper OBJECT_MAPPER = ObjectMappers.get();
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

    public String getFastQ(final long sampleId) {
        return getBySampleId(sampleId, api().path(FASTQ));
    }

    public String getSet(final int sampleId) {
        return getBySampleId(sampleId, api().path("sets"));
    }

    public String getRun(final int id) {
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

    public String getSample(final long sampleId) {
        Response response = sample().path(valueOf(sampleId)).request().buildGet().invoke();
        return returnOrThrow(response);
    }

    public String getDataset(final String biopsyName) {
        return returnOrThrow(api().path("datasets").path(biopsyName).queryParam("output", "condensed").request().get());
    }

    public WebTarget sample() {
        return api().path(SAMPLES);
    }

    public String getSample(final String setId) {
        Response response = sample().queryParam("set_id", setId).request().buildGet().invoke();
        return returnOrThrow(response);
    }

    public List<SbpSample> getSamplesByBiopsy(final String biopsyName) {
        List<SbpSample> samples;
        try {
            samples = ObjectMappers.get()
                    .readValue(returnOrThrow(sample().queryParam("biopsy", biopsyName).request().buildGet().invoke()),
                            new TypeReference<List<SbpSample>>() {
                            });
            return samples.stream().<List<SbpSet>>map(s1 -> readValue(getSet(s1.id()), new TypeReference<>() {
            })).flatMap(Collection::stream)
                    .map(s -> Map.entry(s, readValue(getSample(s.id()), new TypeReference<List<SbpSample>>() {
                    })))
                    .filter(e -> e.getValue().size() == 2)
                    .min((e1, e2) -> e2.getKey().createTime().compareTo(e1.getKey().createTime()))
                    .map(Map.Entry::getValue)
                    .orElseThrow(() -> new IllegalArgumentException("No set found for biopsy [%s] with both a "
                            + "tumor and ref sample. Unable to rerun pipeline with this sample"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public <T> T readValue(final String json, final TypeReference<T> valueTypeRef) {
        try {
            return ObjectMappers.get().readValue(json, valueTypeRef);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void updateRunResult(final String runID, final SbpRunResultUpdate update) {
        LOGGER.info("Patching {} id [{}] with status [{}]", SbpRestApi.RUNS, runID, update);
        returnOrThrow(api().path(RUNS).path(runID).request().build("PATCH", jsonEntity(update)).invoke());
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

    public static <T> Entity<String> jsonEntity(final T payload) {
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

    private String getBySampleId(final long sampleId, final WebTarget path) {
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
