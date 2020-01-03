package com.hartwig.bcl2fastq.stats;

import java.util.function.ToLongFunction;
import java.util.stream.Collectors;

import com.hartwig.bcl2fastq.results.FastqId;

public class Aggregations {

    public static long yield(String barcode, Stats stats) {
        return stats.conversionResults()
                .stream()
                .flatMap(l -> l.demuxResults().stream())
                .collect(Collectors.groupingBy(SampleStats::barcode, Collectors.summingLong(SampleStats::yield)))
                .get(barcode);
    }

    public static long yieldQ30(String barcode, Stats stats) {
        return stats.conversionResults()
                .stream()
                .flatMap(l -> l.demuxResults().stream())
                .collect(Collectors.groupingBy(SampleStats::barcode,
                        Collectors.summingLong(s -> s.readMetrics().stream().mapToLong(ReadMetrics::yieldQ30).sum())))
                .get(barcode);
    }

    public static long yield(FastqId fastqId, Stats stats) {
        return aggregateFastq(fastqId, stats, ReadMetrics::yield);
    }

    public static long yieldQ30(FastqId fastqId, Stats stats) {
        return aggregateFastq(fastqId, stats, ReadMetrics::yieldQ30);
    }

    public static long aggregateFastq(final FastqId fastqId, final Stats stats, final ToLongFunction<ReadMetrics> toLongFunction) {
        Long result = stats.conversionResults()
                .stream()
                .flatMap(c -> c.demuxResults().stream().map(s -> LaneAndSample.of(c.laneNumber(), s)))
                .collect(Collectors.groupingBy(LaneAndSample::toFastqId,
                        Collectors.summingLong(ls -> ls.sample().readMetrics().stream().mapToLong(toLongFunction).sum())))
                .get(fastqId);
        if (result == null){
            throw new IllegalStateException("No stats were found for fastq [{}]. This inconsistency between the files produces and stats "
                    + "is unexpected and should be investigated.");
        }
        return result;
    }
}
