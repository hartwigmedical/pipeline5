package com.hartwig.pipeline.metadata;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.PipelineState;
import com.hartwig.pipeline.sbpapi.SbpRestApi;
import com.hartwig.pipeline.sbpapi.SbpSample;

import org.jetbrains.annotations.NotNull;

public class BiopsyMetadataApi implements SomaticMetadataApi {

    private final SbpRestApi restApi;
    private final ObjectMapper objectMapper;
    private final String biopsyName;
    private final Arguments arguments;

    public BiopsyMetadataApi(final SbpRestApi restApi, final ObjectMapper objectMapper, final String biopsyName,
            final Arguments arguments) {
        this.restApi = restApi;
        this.objectMapper = objectMapper;
        this.biopsyName = biopsyName;
        this.arguments = arguments;
    }

    @Override
    public SomaticRunMetadata get() {
        try {
            List<SbpSample> samples = objectMapper.readValue(restApi.getSamplesByBiopsy(biopsyName), new TypeReference<List<SbpSample>>() {
            });
            Map<String, SbpSample> samplesByType = samples.stream().collect(Collectors.toMap(SbpSample::type, Function.identity()));
            SbpSample tumor = samplesByType.get("tumor");
            SbpSample ref = samplesByType.get("ref");
            if (tumor == null || ref == null) {
                throw new IllegalArgumentException(String.format(
                        "Biopsy [%s] was missing its [%s] sample. This biopsy cannot be used to run a pipeline",
                        biopsyName,
                        tumor == null ? "tumor" : "ref"));
            }
            return SomaticRunMetadata.builder()
                    .bucket(arguments.outputBucket())
                    .set(biopsyName)
                    .maybeTumor(singleSample(tumor, SingleSampleRunMetadata.SampleType.TUMOR))
                    .reference(singleSample(ref, SingleSampleRunMetadata.SampleType.REFERENCE))
                    .build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @NotNull
    public ImmutableSingleSampleRunMetadata singleSample(final SbpSample sample, final SingleSampleRunMetadata.SampleType type) {
        return SingleSampleRunMetadata.builder()
                .type(type)
                .barcode(sample.barcode())
                .set(biopsyName)
                .sampleName(sample.name())
                .bucket(arguments.outputBucket())
                .build();
    }

    @Override
    public void complete(final PipelineState state, final SomaticRunMetadata metadata) {

    }
}
