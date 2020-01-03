package com.hartwig.bcl2fastq.results;

import java.util.List;
import java.util.stream.Collectors;

import org.immutables.value.Value;

@Value.Immutable
public interface Conversion {

    String flowcell();

    long undeterminedReads();

    long totalReads();

    List<ConvertedSample> samples();

    static Conversion from(List<String> paths) {
        return ImmutableConversion.builder()
                .addAllSamples(paths.stream()
                        .collect(Collectors.groupingBy(Conversion::parseBarcode))
                        .entrySet()
                        .stream()
                        .map(ConvertedSample::from)
                        .collect(Collectors.toList()))
                .build();
    }

    static String parseBarcode(String path) {
        return path.split("/")[3];
    }
}
