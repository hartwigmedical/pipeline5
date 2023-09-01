package com.hartwig.pipeline.labels;

import java.util.Optional;

import com.hartwig.pipeline.input.SomaticRunMetadata;
import com.hartwig.pipeline.CommonArguments;
import com.hartwig.pipeline.input.SingleSampleRunMetadata;

public final class LabelsUtil {
    private LabelsUtil() {
    }

    public static Labels fromArguments(CommonArguments arguments) {
        return fromArguments(arguments, null);
    }

    public static Labels fromArguments(CommonArguments arguments, SomaticRunMetadata metadata) {
        String sampleString = null;
        if (metadata != null) {
            sampleString = metadata.maybeTumor()
                    .map(SingleSampleRunMetadata::sampleName)
                    .orElseGet(() -> metadata.maybeReference().map(SingleSampleRunMetadata::sampleName).orElseThrow());
        }

        return ImmutableLabels.of(Optional.ofNullable(sampleString), arguments.runTag(), arguments.userLabel(), arguments.costCenterLabel());
    }
}
