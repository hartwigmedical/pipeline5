package com.hartwig.pipeline.labels;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;
import com.hartwig.pipeline.input.SingleSampleRunMetadata;
import com.hartwig.pipeline.input.SomaticRunMetadata;

import org.immutables.value.Value;
import org.immutables.value.Value.Style.ImplementationVisibility;

@Value.Immutable(builder = false)
@Value.Style(allParameters = true,
             visibility = ImplementationVisibility.PACKAGE)
public interface Labels {

    Optional<String> sample();

    Optional<String> runId();

    Optional<String> user();

    Optional<String> costCenter();

    static Labels createEmpty() {
        return ImmutableLabels.of(Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
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