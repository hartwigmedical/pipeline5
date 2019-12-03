package com.hartwig.bcl2fastq.qc;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.Maps;

public class FastqMinimumQ30 implements FastqQualityCheck {
    @Override
    public Map<String, QualityControlResult> apply(final Stats stats) {

        Map<LaneAndSample, Long> yieldQ30ByLaneAndSample = stats.conversionResults()
                .stream()
                .flatMap(c -> c.demuxResults().stream().map(s -> LaneAndSample.of(c.laneNumber(), s)))
                .collect(Collectors.groupingBy(Function.identity(),
                        Collectors.summingLong(ls -> ls.sample().readMetrics().stream().mapToLong(ReadMetrics::yieldQ30).sum())));

        return Maps.newConcurrentMap();
    }

    private interface LaneAndSample {

        int lane();

        SampleStats sample();

        static LaneAndSample of(int lane, SampleStats sample) {
            return null;
        }

    }
}