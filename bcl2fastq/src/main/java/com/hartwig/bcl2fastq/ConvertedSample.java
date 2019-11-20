package com.hartwig.bcl2fastq;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.immutables.value.Value;

@Value.Immutable
public interface ConvertedSample {

    String barcode();

    List<ConvertedFastq> fastq();

    static ConvertedSample from(Map.Entry<String, List<String>> samplePaths) {
        return ImmutableConvertedSample.builder()
                .barcode(samplePaths.getKey())
                .fastq(samplePaths.getValue()
                        .stream()
                        .collect(Collectors.groupingBy(ConvertedSample::parseLane))
                        .entrySet()
                        .stream()
                        .map(ConvertedFastq::from)
                        .collect(Collectors.toList()))
                .build();
    }

    static String parseLane(String path) {
        return new File(path).getName().split("_")[2];
    }
}
