package com.hartwig.bcl2fastq.stats;

import java.util.stream.Collectors;

public class Aggregations {

    public static long yield(String barcode, Stats stats) {
        return stats.conversionResults()
                .stream()
                .flatMap(l -> l.demuxResults().stream())
                .collect(Collectors.groupingBy(SampleStats::sampleId, Collectors.summingLong(SampleStats::yield)))
                .get(barcode);
    }

    public static long yieldQ30(String barcode, Stats stats) {
        return stats.conversionResults()
                .stream()
                .flatMap(l -> l.demuxResults().stream())
                .collect(Collectors.groupingBy(SampleStats::sampleId,
                        Collectors.summingLong(s -> s.readMetrics().stream().mapToLong(ReadMetrics::yieldQ30).sum())))
                .get(barcode);
    }
}
