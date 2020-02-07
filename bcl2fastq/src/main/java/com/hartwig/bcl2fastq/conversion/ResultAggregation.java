package com.hartwig.bcl2fastq.conversion;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import static com.hartwig.bcl2fastq.stats.Aggregations.yield;
import static com.hartwig.bcl2fastq.stats.Aggregations.yieldQ30;

import java.io.File;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.cloud.storage.Blob;
import com.hartwig.bcl2fastq.samplesheet.IlluminaSample;
import com.hartwig.bcl2fastq.samplesheet.SampleSheet;
import com.hartwig.bcl2fastq.stats.LaneStats;
import com.hartwig.bcl2fastq.stats.SampleStats;
import com.hartwig.bcl2fastq.stats.Stats;
import com.hartwig.bcl2fastq.stats.UndeterminedStats;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.storage.RuntimeBucket;

public class ResultAggregation {

    private final RuntimeBucket bucket;
    private final ResultsDirectory resultsDirectory;

    public ResultAggregation(final RuntimeBucket bucket, final ResultsDirectory resultsDirectory) {
        this.bucket = bucket;
        this.resultsDirectory = resultsDirectory;
    }

    public Conversion apply(SampleSheet sampleSheet, Stats stats) {
        ImmutableConversion.Builder conversionBuilder = ImmutableConversion.builder();
        for (IlluminaSample sample : sampleSheet.samples()) {
            List<Blob> listResult = bucket.list(resultsDirectory.path(String.format("%s/%s", sample.project(), sample.barcode())));
            conversionBuilder.addSamples(ImmutableConvertedSample.builder()
                    .barcode(sample.barcode())
                    .sample(sample.sample())
                    .project(sample.project())
                    .addAllFastq(listResult != null ? listResult.stream()
                            .collect(groupingBy(b -> parseLane(b.getName())))
                            .entrySet()
                            .stream()
                            .map(e -> ResultAggregation.fastq(sample, e, stats))
                            .sorted(Comparator.comparingInt(o -> o.id().lane()))
                            .collect(toList()) : Collections.emptyList())
                    .build());
        }
        return conversionBuilder.undeterminedReads(stats.conversionResults()
                .stream()
                .map(LaneStats::undetermined)
                .mapToLong(UndeterminedStats::yield)
                .sum())
                .totalReads(stats.conversionResults().stream().flatMap(c -> c.demuxResults().stream()).mapToLong(SampleStats::yield).sum()
                        + stats.conversionResults().stream().mapToLong(c -> c.undetermined().yield()).sum())
                .flowcell(stats.flowcell())
                .build();
    }

    static String parseLane(String path) {
        return new File(path).getName().split("_")[2];
    }

    static ConvertedFastq fastq(IlluminaSample sample, Map.Entry<String, List<Blob>> lane, Stats stats) {
        Map<String, Blob> pair =
                lane.getValue().stream().collect(toMap(b -> ResultAggregation.parseNumInPair(b.getName()), Function.identity()));
        Blob blobR1 = pair.get("R1");
        Blob blobR2 = pair.get("R2");
        if (blobR1 == null || blobR2 == null) {
            throw new IllegalArgumentException(String.format("Missing one or both ends of pair in lane [%s] paths [%s]",
                    lane.getKey(),
                    lane.getValue().stream().map(Blob::getName).collect(Collectors.joining(","))));
        }
        FastqId id = FastqId.of(parseLaneIndex(blobR1.getName()), sample.barcode());
        return ImmutableConvertedFastq.builder()
                .id(id)
                .pathR1(blobR1.getName())
                .pathR2(blobR2.getName())
                .outputPathR1(outputPath(stats, blobR1))
                .outputPathR2(outputPath(stats, blobR2))
                .sizeR1(blobR1.getSize())
                .sizeR2(blobR2.getSize())
                .md5R1(blobR1.getMd5())
                .md5R2(blobR2.getMd5())
                .yield(yield(id, stats))
                .yieldQ30(yieldQ30(id, stats))
                .build();
    }

    private static String outputPath(final Stats stats, final Blob blobR1) {
        String[] fileNameSplit = new File(blobR1.getName()).getName().split("_");
        return String.format("%s_%s_%s_%s_%s_%s",
                fileNameSplit[0],
                stats.flowcell(),
                fileNameSplit[1],
                fileNameSplit[2],
                fileNameSplit[3],
                fileNameSplit[4]);
    }

    static String parseNumInPair(String path) {
        return part(path, 3);
    }

    static int parseLaneIndex(String path) {
        return Integer.parseInt(part(path, 2).replace("L", ""));
    }

    static String part(final String path, final int i) {
        return new File(path).getName().split("_")[i];
    }
}
