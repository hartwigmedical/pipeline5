package com.hartwig.pipeline.metadata;

import com.hartwig.patient.ReferenceTumorPair;

public class ModeResolver {

    InputMode apply(final SomaticRunMetadata metadata) {
        return metadata.maybeTumor()
                .map(t -> metadata.maybeReference().map(r -> InputMode.SOMATIC).orElse(InputMode.TUMOR_ONLY))
                .orElseGet(() -> metadata.maybeReference().map(r -> InputMode.GERMLINE_ONLY).orElseThrow());
    }
}