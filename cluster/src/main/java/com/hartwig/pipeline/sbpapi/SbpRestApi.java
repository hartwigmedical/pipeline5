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

public class SbpRestApi {

    private final static ObjectMapper OBJECT_MAPPER = ObjectMappers.get();
    private static final String SAMPLES = "samples";
    private static final String FILES = "files";
    private static final String FASTQ = "fastq";
    private final WebTarget target;

    private SbpRestApi(final WebTarget target) {
        this.target = target;
    }

    public String getFastQ(final long sampleId) {
        return getBySampleId(sampleId, api().path(FASTQ));
    }

    private String returnOrThrow(final Response response) {
        if (response.getStatusInfo().getFamily() == Response.Status.Family.SUCCESSFUL) {
            return response.readEntity(String.class);
        }
        throw error(response);
    }

    public String getSample(final long sampleId) {
        Response response = sample().path(valueOf(sampleId)).request().buildGet().invoke();
        return returnOrThrow(response);
    }

    public String getDataset(final String biopsyName) {
        return returnOrThrow(api().path("datasets").path(biopsyName).queryParam("output", "condensed").request().get());
    }

    public String getLane(final long lane_id) {
        return returnOrThrow(api().path("lanes").path(valueOf(lane_id)).request().get());
    }

    public WebTarget sample() {
        return api().path(SAMPLES);
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
