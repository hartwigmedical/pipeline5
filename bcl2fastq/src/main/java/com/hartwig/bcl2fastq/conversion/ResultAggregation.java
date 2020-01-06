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
import java.util.stream.StreamSupport;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.bcl2fastq.samplesheet.IlluminaSample;
import com.hartwig.bcl2fastq.samplesheet.SampleSheet;
import com.hartwig.bcl2fastq.stats.LaneStats;
import com.hartwig.bcl2fastq.stats.SampleStats;
import com.hartwig.bcl2fastq.stats.Stats;
import com.hartwig.bcl2fastq.stats.UndeterminedStats;

public class ResultAggregation {

    private final Bucket bucket;

    public ResultAggregation(final Bucket bucket) {
        this.bucket = bucket;
    }

    public Conversion apply(SampleSheet sampleSheet, Stats stats) {
        ImmutableConversion.Builder conversionBuilder = ImmutableConversion.builder();
        for (IlluminaSample sample : sampleSheet.samples()) {
            Page<Blob> listResult = bucket.list(Storage.BlobListOption.prefix(String.format("%s/%s", sample.project(), sample.barcode())));
            conversionBuilder.addSamples(ImmutableConvertedSample.builder()
                    .barcode(sample.barcode())
                    .sample(sample.sample())
                    .project(sample.project())
                    .yield(yield(sample.barcode(), stats))
                    .yieldQ30(yieldQ30(sample.barcode(), stats))
                    .addAllFastq(listResult != null ? StreamSupport.stream(listResult.iterateAll().spliterator(), false)
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
                .totalReads(stats.conversionResults().stream().flatMap(c -> c.demuxResults().stream()).mapToLong(SampleStats::yield).sum())
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
                .outputPathR1(outputPath(sample, stats, blobR1))
                .outputPathR2(outputPath(sample, stats, blobR2))
                .sizeR1(blobR1.getSize())
                .sizeR2(blobR2.getSize())
                .md5R1(blobR1.getMd5())
                .md5R2(blobR2.getMd5())
                .yield(yield(id, stats))
                .yieldQ30(yieldQ30(id, stats))
                .build();
    }

    private static String outputPath(final IlluminaSample sample, final Stats stats, final Blob blobR1) {
        return stats.flowcell() + "/" + sample.barcode() + "/" + new File(blobR1.getName()).getName();
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
