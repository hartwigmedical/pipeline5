package com.hartwig.bcl2fastq.metadata;

import static java.lang.String.format;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
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

public class SbpFastqMetadataApi {

    private final static Logger LOGGER = LoggerFactory.getLogger(SbpFastqMetadataApi.class);

    private static final String FLOWCELLS = "flowcells";
    private static final String SAMPLES = "samples";
    private static final String FASTQ = "fastq";
    private final WebTarget target;
    private final ObjectMapper objectMapper = ObjectMappers.get();

    private SbpFastqMetadataApi(final WebTarget target) {
        this.target = target;
    }

    public SbpFlowcell getFlowcell(String id) {
        try {
            return findOne(api().path(FLOWCELLS).queryParam("flowcell_id", id).request(), new TypeReference<List<SbpFlowcell>>() {
            }).orElseThrow(() -> new IllegalArgumentException(String.format(
                    "No flowcell found for id [%s] Has it been registered with the SBP API?",
                    id)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public SbpSample findOrCreate(String barcode, String submission) {
        try {
            return findOne(samples().queryParam("barcode", barcode).request(), new TypeReference<List<SbpSample>>() {
            }).orElseGet(() -> {
                SbpSample sample = SbpSample.builder().barcode(barcode).status("Unregistered").submission(submission).build();
                try {
                    Response response = samples().request()
                            .post(Entity.entity(objectMapper.writeValueAsString(sample), MediaType.APPLICATION_JSON_TYPE));
                    if (isSuccessful(response)) {
                        return findOrCreate(barcode, submission);
                    } else {
                        throw new RuntimeException(String.format("Unable to post new sample [%s] api returned status [%s] and message [%s]",
                                barcode,
                                response.getStatus(),
                                response.readEntity(String.class)));
                    }
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean isSuccessful(final Response response) {
        return response.getStatusInfo().getFamily() == Response.Status.Family.SUCCESSFUL;
    }

    public void create(SbpFastq fastq) {
        try {
            Optional<SbpFastq> existing = findFastq(fastq.sample_id()).stream()
                    .filter(f -> f.lane_id() == fastq.lane_id())
                    .filter(f -> f.bucket().equals(fastq.bucket()))
                    .filter(f -> f.name_r1().equals(fastq.name_r1()))
                    .filter(f -> f.name_r2().equals(fastq.name_r2()))
                    .findFirst();

            if (existing.isPresent()) {
                SbpFastq update = SbpFastq.builder().from(fastq).id(existing.get().id()).build();
                String fastqJson = objectMapper.writeValueAsString(update);
                returnOrThrow(fastq().path(existing.get().id().orElseThrow().toString())
                        .request()
                        .build("PATCH", Entity.entity(fastqJson, MediaType.APPLICATION_JSON_TYPE))
                        .invoke());
            } else {
                String fastqJson = objectMapper.writeValueAsString(fastq);
                returnOrThrow(fastq().request().post(Entity.entity(fastqJson, MediaType.APPLICATION_JSON_TYPE)));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private List<SbpFastq> findFastq(final int sampleId) throws IOException {
        return objectMapper.readValue(returnOrThrow(fastq().queryParam("sample_id", sampleId).request().get()),
                new TypeReference<List<SbpFastq>>() {
                });
    }

    public SbpLane findOrCreate(final SbpLane sbpLane) {
        try {
            List<SbpLane> lanes = objectMapper.readValue(returnOrThrow(api().path("lanes")
                    .queryParam("name", sbpLane.name())
                    .queryParam("flowcell_id", sbpLane.flowcell_id())
                    .request()
                    .get()), new TypeReference<List<SbpLane>>() {
            });
            if (lanes.isEmpty()) {
                returnOrThrow(api().path("lanes")
                        .request()
                        .post(Entity.entity(objectMapper.writeValueAsString(sbpLane), MediaType.APPLICATION_JSON_TYPE)));
                return findOrCreate(sbpLane);
            } else {
                return lanes.stream().findFirst().orElseThrow();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private WebTarget samples() {
        return api().path(SAMPLES);
    }

    private WebTarget fastq() {
        return api().path(FASTQ);
    }

    public SbpFlowcell updateFlowcell(SbpFlowcell flowcell) {
        try {
            returnOrThrow(api().path(FLOWCELLS)
                    .path(String.valueOf(flowcell.id()))
                    .request()
                    .build("PATCH", Entity.entity(objectMapper.writeValueAsString(flowcell), MediaType.APPLICATION_JSON_TYPE))
                    .invoke());
            return getFlowcell(String.valueOf(flowcell.flowcell_id()));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private <T> Optional<T> findOne(final Invocation.Builder request, final TypeReference<List<T>> reference) throws IOException {
        return objectMapper.<List<T>>readValue(returnOrThrow(request.get()), reference).stream().findFirst();
    }

    private WebTarget api() {
        return target.path("hmf").path("v1");
    }

    private String returnOrThrow(final Response response) {
        if (isSuccessful(response)) {
            return response.readEntity(String.class);
        }
        LOGGER.error(response.readEntity(String.class));
        throw error(response);
    }

    private RuntimeException error(final Response response) {
        return new RuntimeException(format("Received an error status result [%s] of SBP Api at [%s]",
                response.getStatus(),
                target.getUri()));
    }

    public static SbpFastqMetadataApi newInstance(final String url) {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.property(HttpUrlConnectorProvider.SET_METHOD_WORKAROUND, true);
        return new SbpFastqMetadataApi(ClientBuilder.newBuilder().withConfig(clientConfig).build().target(url));
    }

    public void updateSample(final SbpSample sample) {
        try {
            Integer id = sample.id().orElseThrow();
            returnOrThrow(api().path(SAMPLES)
                    .path(String.valueOf(id))
                    .request()
                    .build("PATCH", Entity.entity(objectMapper.writeValueAsString(sample), MediaType.APPLICATION_JSON_TYPE))
                    .invoke());
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public List<SbpFastq> getFastqs(final SbpSample sbpSample) {
        try {
            return findFastq(sbpSample.id().orElseThrow());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
