package com.hartwig.pipeline.metadata;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.execution.PipelineStatus;

public class LocalSampleMetadataApi implements SampleMetadataApi {

    private static final String NO_BARCODE = "none";
    private final Arguments arguments;
    private final LocalDateTime timestamp;

    LocalSampleMetadataApi(final Arguments arguments, final LocalDateTime timestamp) {
        this.arguments = arguments;
        this.timestamp = timestamp;
    }

    public SampleMetadata get() {
        return arguments.runId()
                .map(runId -> trim(arguments.sampleId()) + "-" + runId)
                .map(setId -> SampleMetadata.builder().barcodeOrSampleName(arguments.sampleId()).setName(setId))
                .orElse(SampleMetadata.builder()
                        .barcodeOrSampleName(arguments.sampleId())
                        .setName(trim(arguments.sampleId()) + "-" + timestamp.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))))
                .build();
    }

    @Override
    public void complete(PipelineStatus status) {
        // do nothing
    }

    private String trim(final String sampleId) {
        return sampleId.substring(0, arguments.sampleId().length() - 1);
    }
}
