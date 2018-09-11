package com.hartwig.pipeline.upload;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hartwig.patient.ImmutableLane;
import com.hartwig.patient.Lane;
import com.hartwig.patient.Patient;
import com.hartwig.patient.Sample;
import com.hartwig.patient.io.PatientReader;

import org.jetbrains.annotations.NotNull;

class SBPPatientReader {

    private static final String DIRECTORY = "hmf-fastq-storage";
    private final SBPRestApi sbpRestApi;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    static {
        OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    SBPPatientReader(final SBPRestApi sbpRestApi) {
        this.sbpRestApi = sbpRestApi;
    }

    Patient read(final String patientId) {
        Optional<String> maybeFastQ = sbpRestApi.getFastQ(patientId);
        return maybeFastQ.map(json -> {
            try {
                List<FastQMetadata> fastqJson = parseJson(json);
                Optional<Sample> maybeReference = sample(patientId, fastqJson, PatientReader.TypeSuffix.REFERENCE);
                if (maybeReference.isPresent()) {
                    Optional<Sample> maybeTumor = sample(patientId, fastqJson, PatientReader.TypeSuffix.TUMOR);
                    return maybeTumor.map(tumor -> Patient.of(DIRECTORY, patientId, maybeReference.get(), tumor))
                            .orElse(Patient.of(DIRECTORY, patientId, maybeReference.get()));
                }
                throw new IllegalStateException("No reference sample was found for patient [" + patientId + "] although fastqs do exist");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).orElseThrow(() -> new IllegalArgumentException("Unable to find patient [{}] in SBP API"));
    }

    @NotNull
    private Optional<Sample> sample(final String patientId, final List<FastQMetadata> fastqJson, final PatientReader.TypeSuffix suffix) {
        List<Lane> lanes = fastqJson.stream()
                .filter(fastQMetadata -> fastQMetadata.name_r1().startsWith(patientId + suffix.getSuffix()))
                .map(SBPPatientReader::lane)
                .collect(Collectors.toList());
        if (lanes.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(Sample.builder(DIRECTORY, patientId + "R").addAllLanes(lanes).build());
    }

    @NotNull
    private static ImmutableLane lane(final FastQMetadata fastQMetadata) {
        return Lane.builder()
                .name("")
                .readsPath(fastQMetadata.name_r1())
                .matesPath(fastQMetadata.name_r2())
                .directory(DIRECTORY)
                .suffix("")
                .flowCellId("")
                .index("0")
                .build();
    }

    private List<FastQMetadata> parseJson(final String json) throws IOException {
        return OBJECT_MAPPER.readValue(json, new TypeReference<List<FastQMetadata>>() {
        });
    }
}
