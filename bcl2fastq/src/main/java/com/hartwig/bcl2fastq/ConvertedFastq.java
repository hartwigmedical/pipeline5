package com.hartwig.bcl2fastq;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.immutables.value.Value;

@Value.Immutable
public interface ConvertedFastq {

    String pathR1();

    String pathR2();

    static ConvertedFastq from(Map.Entry<String, List<String>> lane) {
        Map<String, String> pair = lane.getValue().stream().collect(Collectors.toMap(ConvertedFastq::parseNumInPair, Function.identity()));
        String pathR1 = pair.get("R1");
        String pathR2 = pair.get("R2");
        if (pathR1 == null || pathR2 == null) {
            throw new IllegalArgumentException(String.format("Missing one or both ends of pair in lane [%s] paths [%s]",
                    lane.getKey(),
                    String.join(",", lane.getValue())));
        }
        return ImmutableConvertedFastq.builder().pathR1(pathR1).pathR2(pathR2).build();
    }

    static String parseNumInPair(String path) {
        return new File(path).getName().split("_")[3];
    }
}
