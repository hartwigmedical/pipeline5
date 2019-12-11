package com.hartwig.bcl2fastq.qc;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.hartwig.bcl2fastq.FastqId;
import com.hartwig.bcl2fastq.stats.ReadMetrics;
import com.hartwig.bcl2fastq.stats.Stats;

public class FastqMinimumQ30 implements FastqQualityCheck {

    static final String QC_NAME = "Minimum Q30 not met";
    private final long minimumQ30;

    FastqMinimumQ30(final long minimumQ30) {
        this.minimumQ30 = minimumQ30;
    }

    @Override
    public Map<FastqId, QualityControlResult> apply(final Stats stats) {
        return stats.conversionResults()
                .stream()
                .flatMap(c -> c.demuxResults().stream().map(s -> LaneAndSample.of(c.laneNumber(), s)))
                .collect(Collectors.groupingBy(Function.identity(),
                        Collectors.summingLong(ls -> ls.sample().readMetrics().stream().mapToLong(ReadMetrics::yieldQ30).sum()))).entrySet()
                .stream()
                .collect(Collectors.toMap(e -> e.getKey().toFastqId(),
                        e -> QualityControlResult.of(QC_NAME, e.getValue() > minimumQ30)));
    }
}