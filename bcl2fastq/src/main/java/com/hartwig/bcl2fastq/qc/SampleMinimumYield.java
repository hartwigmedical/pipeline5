package com.hartwig.bcl2fastq.qc;

import java.util.Map;
import java.util.stream.Collectors;

public class SampleMinimumYield implements SampleQualityCheck {

    private final long minimumYield;

    public SampleMinimumYield(final long minimumYield) {
        this.minimumYield = minimumYield;
    }

    @Override
    public Map<String, QualityControlResult> apply(final Stats stats) {
        return stats.conversionResults()
                .stream()
                .flatMap(l -> l.demuxResults().stream())
                .filter(s -> s.sampleId().isPresent())
                .collect(Collectors.groupingBy(s -> s.sampleId().orElse("n/a"), Collectors.summingDouble(SampleStats::yield))).entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey,
                        e -> QualityControlResult.of("Sample minumum yield", e.getValue() > minimumYield)));
    }
}
