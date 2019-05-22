package com.hartwig.pipeline.metadata;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import com.hartwig.pipeline.Arguments;

public class PatientMetadataApi {

    private final Arguments arguments;
    private final LocalDateTime timestamp;

    PatientMetadataApi(final Arguments arguments, final LocalDateTime timestamp) {
        this.arguments = arguments;
        this.timestamp = timestamp;
    }

    public PatientMetadata getMetadata() {
        return arguments.runId()
                .map(runId -> arguments.sampleId() + "-" + runId)
                .map(PatientMetadata::of)
                .orElse(PatientMetadata.of(
                        arguments.sampleId() + "-" + timestamp.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))));
    }
}
