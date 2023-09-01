package com.hartwig.pipeline.input;

public class ModeResolver {

    public InputMode apply(final SomaticRunMetadata metadata) {
        return metadata.maybeTumor()
                .map(t -> metadata.maybeReference().map(r -> InputMode.TUMOR_REFERENCE).orElse(InputMode.TUMOR_ONLY))
                .orElseGet(() -> metadata.maybeReference().map(r -> InputMode.REFERENCE_ONLY).orElseThrow());
    }
}