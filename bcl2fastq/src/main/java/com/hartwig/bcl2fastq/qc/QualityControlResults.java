package com.hartwig.bcl2fastq.qc;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.hartwig.bcl2fastq.FastqId;

import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Value.Immutable
public interface QualityControlResults {

    Logger LOGGER = LoggerFactory.getLogger(QualityControlResults.class);

    List<QualityControlResult> flowcellLevel();

    Map<String, Collection<QualityControlResult>> sampleLevel();

    Map<FastqId, Collection<QualityControlResult>> fastqLevel();

    default boolean flowcellPasses(final String flowcell) {
        return evaluate(flowcell, flowcellLevel());
    }

    default boolean samplePasses(final String sampleId) {
        return evaluate(sampleId, sampleLevel().get(sampleId));
    }

    default boolean fastqPasses(final FastqId id) {
        return evaluate(id.toString(), fastqLevel().get(id));
    }

    default boolean evaluate(final String id, final Collection<QualityControlResult> qualityControlResults) {
        boolean pass = qualityControlResults.stream().allMatch(QualityControlResult::pass);
        if (!pass) {
            LOGGER.warn("[{}] failed qc checks: {} ",
                    id,
                    qualityControlResults.stream()
                            .filter(QualityControlResult::fail)
                            .map(QualityControlResult::name)
                            .collect(Collectors.joining(",")));
        }
        return pass;
    }

    static ImmutableQualityControlResults.Builder builder() {
        return ImmutableQualityControlResults.builder();
    }
}
