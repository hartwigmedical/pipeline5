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
        if (!arguments.sbpApiSampleId().isPresent()) {
            return arguments.runId()
                    .map(runId -> trim(arguments.sampleId()) + "-" + runId)
                    .map(runId -> PatientMetadata.of(arguments.sampleId(), runId))
                    .orElse(PatientMetadata.of(arguments.sampleId(),
                            trim(arguments.sampleId()) + "-" + timestamp.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))));
        } else {
            return PatientMetadata.of(String.valueOf(arguments.sbpApiSampleId().get()), "sbp-test-set-" + arguments.sbpApiSampleId().get());
        }
    }

    private String trim(final String sampleId) {
        return sampleId.substring(0, arguments.sampleId().length() - 1);
    }
}
