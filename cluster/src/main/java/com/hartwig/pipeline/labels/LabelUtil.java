package com.hartwig.pipeline.labels;

import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.input.SomaticRunMetadata;

public final class LabelUtil {

    private LabelUtil() {

    }

    public static Map<String, String> createLabels(Arguments arguments, SomaticRunMetadata metadata) {
        String sampleString;
        if (metadata.maybeTumor().isPresent()) { // can be written in functional style but is less readable.
            sampleString = metadata.maybeTumor().get().sampleName();
        } else if (metadata.maybeReference().isPresent()) {
            sampleString = metadata.maybeReference().get().sampleName();
        } else {
            throw new IllegalArgumentException("No sample string found, cannot construct labels.");
        }
        ImmutableMap.Builder<String, String> mapBuilder = ImmutableMap.builder();
        mapBuilder.put("sample", sampleString);
        arguments.runTag().ifPresent(l -> mapBuilder.put("run_id", cleanLabel(l)));
        arguments.userLabel().ifPresent(l -> mapBuilder.put("user", cleanLabel(l)));
        arguments.costCenterLabel().ifPresent(l -> mapBuilder.put("cost_center", cleanLabel(l)));
        return mapBuilder.build();
    }

    private static String cleanLabel(String label) {
        return label.toLowerCase().replace("_", "-").replace('.', '-');
    }
}
