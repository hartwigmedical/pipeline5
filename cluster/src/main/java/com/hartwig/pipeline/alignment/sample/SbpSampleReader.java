package com.hartwig.pipeline.alignment.sample;

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
import com.hartwig.pipeline.sbpapi.SbpFastQ;
import com.hartwig.pipeline.sbpapi.SbpRestApi;
import com.hartwig.pipeline.sbpapi.SbpSample;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SbpSampleReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(SbpSampleReader.class);
    private final SbpRestApi sbpRestApi;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    static {
        OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    public SbpSampleReader(final SbpRestApi sbpRestApi) {
        this.sbpRestApi = sbpRestApi;
    }

    public Sample read(final int sampleId) {
        String rawFastQJson = sbpRestApi.getFastQ(sampleId);
        try {
            List<SbpFastQ> fastqJson = parseFastqJson(rawFastQJson);
            String sampleName = extract(sampleId, fastqJson, name());
            String barcode = parseSampleJson(sbpRestApi.getSample(sampleId)).barcode();
            Sample sample = sample(sampleName, barcode, fastqJson);
            LOGGER.info("Found sample [{}] in SBP", sample);
            if (sample.lanes().isEmpty()) {
                throw new IllegalArgumentException(String.format(
                        "No lanes (fastq) were found for sample [%s]. Either no fastq files in the api or none pass qc",
                        sample.name()));
            }
            return sample;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Function<String, String> name() {
        return fileName -> fileName.substring(0, fileName.indexOf("_"));
    }

    private String extract(final int sampleId, final List<SbpFastQ> fastqJson, final Function<String, String> stringFunction) {
        return fastqJson.stream()
                .map(SbpFastQ::name_r1)
                .map(SbpSampleReader::removePath)
                .map(stringFunction)
                .distinct()
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(format("No FASTQ available in SBP object store for sample [%s]",
                        sampleId)));
    }

    private static String removePath(String name) {
        String[] split = name.split("/");
        return split[split.length - 1];
    }

    private Sample sample(final String sampleName, final String barcode, final List<SbpFastQ> fastqJson) {
        List<Lane> lanes = fastqJson.stream().filter(SbpSampleReader::qcPass).map(SbpSampleReader::lane).collect(Collectors.toList());
        return Sample.builder("", sampleName, barcode).addAllLanes(lanes).build();
    }

    private static boolean qcPass(SbpFastQ sbpFastQ) {
        if (!sbpFastQ.qc_pass()) {
            LOGGER.warn("FastQ file [{}] did not pass QC, filtering from sample.", sbpFastQ.name_r1());
            return false;
        }
        return true;
    }

    private static ImmutableLane lane(final SbpFastQ sbpFastQ) {
        String bucket = sbpFastQ.bucket();
        if (bucket == null || bucket.isEmpty()) {
            throw new IllegalStateException(format("Bucket for fastq [%s] was null or empty. Has this sample id been cleaned up in S3?",
                    sbpFastQ));
        }
        String[] tokens = sbpFastQ.name_r1().split("_");
        String laneNumber = tokens[3];
        return Lane.builder()
                .name("")
                .laneNumber(laneNumber)
                .firstOfPairPath(s3Path(sbpFastQ, sbpFastQ.name_r1()))
                .secondOfPairPath(s3Path(sbpFastQ, sbpFastQ.name_r2()))
                .directory("")
                .suffix("")
                .flowCellId("")
                .index("0")
                .build();
    }

    private static String s3Path(final SbpFastQ sbpFastQ, final String file) {
        return sbpFastQ.bucket() + "/" + file;
    }

    private List<SbpFastQ> parseFastqJson(final String json) throws IOException {
        return OBJECT_MAPPER.readValue(json, new TypeReference<List<SbpFastQ>>() {
        });
    }

    private SbpSample parseSampleJson(final String json) throws IOException {
        return OBJECT_MAPPER.readValue(json, SbpSample.class);
    }
}
