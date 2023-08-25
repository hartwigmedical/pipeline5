package com.hartwig.pipeline.labels;

import com.hartwig.computeengine.input.SomaticRunMetadata;
import com.hartwig.computeengine.labels.Labels;
import com.hartwig.computeengine.labels.LabelsBuilder;
import com.hartwig.pipeline.CommonArguments;

public final class LabelsUtil {
    private LabelsUtil() {
    }

    public static Labels fromArguments(CommonArguments arguments) {
        return fromArguments(arguments, null);
    }

    public static Labels fromArguments(CommonArguments arguments, SomaticRunMetadata metadata) {
        return new LabelsBuilder().userLabel(arguments.userLabel().orElse(null))
                .runTag(arguments.runTag().orElse(null))
                .costCenterLabel(arguments.costCenterLabel().orElse(null))
                .sample(metadata)
                .build();
    }
}
