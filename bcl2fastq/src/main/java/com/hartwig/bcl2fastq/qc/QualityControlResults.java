package com.hartwig.bcl2fastq.qc;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.immutables.value.Value;

@Value.Immutable
public interface QualityControlResults {

    List<QualityControlResult> flowcellLevel();

    Map<String, Collection<QualityControlResult>> sampleLevel();

    default boolean flowcellPasses() {
        return flowcellLevel().stream().allMatch(QualityControlResult::pass);
    }

    static ImmutableQualityControlResults.Builder builder() {
        return ImmutableQualityControlResults.builder();
    }
}
