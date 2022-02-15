package com.hartwig.pipeline.labels;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.google.common.collect.ImmutableMap;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.CommonArguments;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;

import org.immutables.value.Value;
import org.jetbrains.annotations.NotNull;

@Value.Immutable
public interface Labels {

    Optional<String> sample();

    Optional<String> runId();

    Optional<String> user();

    Optional<String> costCenter();

    static Labels of(final CommonArguments arguments, final SomaticRunMetadata sample) {
        return builder(arguments).sample(sample.maybeTumor()
                .map(SingleSampleRunMetadata::sampleName)
                .orElseGet(() -> sample.maybeReference().map(SingleSampleRunMetadata::sampleName).orElseThrow())).build();
    }

    static Labels of(final CommonArguments arguments) {
        return builder(arguments).build();
    }

    private static ImmutableLabels.Builder builder(final CommonArguments arguments) {
        return ImmutableLabels.builder().runId(arguments.runId()).user(arguments.userLabel()).costCenter(arguments.costCenterLabel());
    }

    default Map<String, String> asMap() {
        return asMap(Collections.emptyList());
    }

    default Map<String, String> asMap(final List<Map.Entry<String, String>> additional) {
        ImmutableMap.Builder<String, String> mapBuilder = ImmutableMap.builder();
        sample().ifPresent(l -> mapBuilder.put("sample", clean(l)));
        runId().ifPresent(l -> mapBuilder.put("run_id", clean(l)));
        user().ifPresent(l -> mapBuilder.put("user", clean(l)));
        costCenter().ifPresent(l -> mapBuilder.put("cost_center", clean(l)));
        for (Map.Entry<String, String> entry : additional) {
            mapBuilder.put(entry);
        }
        return mapBuilder.build();
    }

    private static String clean(final String string) {
        return string.toLowerCase().replace("_", "-").replace('.', '-');
    }
}