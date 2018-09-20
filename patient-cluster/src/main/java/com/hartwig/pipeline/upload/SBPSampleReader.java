package com.hartwig.pipeline.upload;

import static java.lang.String.format;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hartwig.patient.ImmutableLane;
import com.hartwig.patient.Lane;
import com.hartwig.patient.Sample;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SBPSampleReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(SBPSampleReader.class);
    private final SBPRestApi sbpRestApi;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    static {
        OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    public SBPSampleReader(final SBPRestApi sbpRestApi) {
        this.sbpRestApi = sbpRestApi;
    }

    public Sample read(final int sampleId) {
        String rawFastQJson = sbpRestApi.getFastQ(sampleId);
        try {
            List<FastQMetadata> fastqJson = parseJson(rawFastQJson);
            String sampleName = extract(sampleId, fastqJson, name());
            String barcode = extract(sampleId, fastqJson, barcode());
            Sample sample = sample(sampleName, barcode, fastqJson);
            LOGGER.info("Found sample [{}] in SBP", sample);
            return sample;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Function<String, String> barcode() {
        return fileName -> fileName.split("_")[1];
    }

    @NotNull
    public Function<String, String> name() {
        return fileName -> fileName.substring(0, fileName.indexOf("_"));
    }

    @NotNull
    private String extract(final int sampleId, final List<FastQMetadata> fastqJson, final Function<String, String> stringFunction) {
        return fastqJson.stream().map(FastQMetadata::name_r1).map(stringFunction)
                .distinct()
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(format("No FASTQ available in SBP object store for sample [%s]",
                        sampleId)));
    }

    @NotNull
    private Sample sample(final String sampleName, final String barcode, final List<FastQMetadata> fastqJson) {
        List<Lane> lanes = fastqJson.stream().filter(FastQMetadata::qc_pass).map(SBPSampleReader::lane).collect(Collectors.toList());
        return Sample.builder("", sampleName, barcode).addAllLanes(lanes).build();
    }

    @NotNull
    private static ImmutableLane lane(final FastQMetadata fastQMetadata) {
        String bucket = fastQMetadata.bucket();
        if (bucket == null || bucket.isEmpty()) {
            throw new IllegalStateException(String.format(
                    "Bucket for fastq [%s] was null or empty. Has this sample id been cleaned up in S3?",
                    fastQMetadata));
        }
        return Lane.builder()
                .name("")
                .readsPath(s3Path(fastQMetadata, fastQMetadata.name_r1()))
                .matesPath(s3Path(fastQMetadata, fastQMetadata.name_r2()))
                .directory("")
                .suffix("")
                .flowCellId("")
                .index("0")
                .build();
    }

    @NotNull
    private static String s3Path(final FastQMetadata fastQMetadata, final String file) {
        return fastQMetadata.bucket() + "/" + file;
    }

    private List<FastQMetadata> parseJson(final String json) throws IOException {
        return OBJECT_MAPPER.readValue(json, new TypeReference<List<FastQMetadata>>() {
        });
    }
}
