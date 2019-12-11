package com.hartwig.bcl2fastq.qc;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.hartwig.bcl2fastq.stats.SampleStats;
import com.hartwig.bcl2fastq.stats.Stats;

public class SampleMinimumYield implements FlowcellQualityCheck {

    private final long minimumYield;

    SampleMinimumYield(final long minimumYield) {
        this.minimumYield = minimumYield;
    }

    @Override
    public QualityControlResult apply(final Stats stats, final String log) {

        List<String> failingSamples =
                stats.conversionResults()
                        .stream()
                        .flatMap(l -> l.demuxResults().stream())
                        .filter(s -> s.sampleId().isPresent())
                        .collect(Collectors.groupingBy(s -> s.sampleId().orElse("n/a"), Collectors.summingDouble(SampleStats::yield)))
                        .entrySet()
                        .stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, e1 -> e1.getValue() > minimumYield))
                        .entrySet().stream().filter(e -> !e.getValue()).map(Map.Entry::getKey).collect(Collectors.toList());

        if (failingSamples.isEmpty()) {
            return QualityControlResult.of("All samples meet minimum yield", true);
        }
        return QualityControlResult.of(String.format("Samples [%s] did not meet minimum yield of [%s]",
                String.join(",", failingSamples),
                minimumYield), false);
    }
}
