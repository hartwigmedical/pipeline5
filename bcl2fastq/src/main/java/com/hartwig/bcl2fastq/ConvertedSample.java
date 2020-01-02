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
        String barcode = samplePaths.getKey();
        return ImmutableConvertedSample.builder()
                .barcode(barcode)
                .fastq(samplePaths.getValue()
                        .stream()
                        .collect(Collectors.groupingBy(ConvertedSample::parseLane))
                        .entrySet()
                        .stream()
                        .map(e -> ConvertedFastq.from(barcode, e))
                        .collect(Collectors.toList()))
                .build();
    }

    static String parseLane(String path) {
        return new File(path).getName().split("_")[2];
    }
}
