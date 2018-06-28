package com.hartwig.pipeline.runtime.patient;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import com.hartwig.patient.ImmutableLane;
import com.hartwig.patient.Lane;
import com.hartwig.patient.Sample;

class Samples {

    static Sample createPairedEndSample(final Path sampleDirectory, final String sampleName, String postfix) throws IOException {
        Map<String, ImmutableLane.Builder> builders = new HashMap<>();
        String sampleNameWithPostfix = sampleName + postfix;
        for (Path path : Files.newDirectoryStream(sampleDirectory, sampleNameWithPostfix + "_*_S?_L*_R?_*.fastq*")) {
            String[] tokens = path.toFile().getName().split("_");
            String laneName = tokens[3];
            ImmutableLane.Builder builder = builders.computeIfAbsent(laneName,
                    s -> Lane.builder()
                            .directory(sampleDirectory.toString())
                            .name(sampleNameWithPostfix + "_" + s)
                            .flowCellId(tokens[1])
                            .index(tokens[2])
                            .suffix(tokens[5].substring(0, tokens[5].indexOf('.'))));
            if (tokens[4].equals("R1")) {
                builder.readsPath(path.toString());
            } else if (tokens[4].equals("R2")) {
                builder.matesPath(path.toString());
            }
            builder.flowCellId(tokens[1]);
        }
        return Sample.builder(sampleDirectory.toString(), sampleNameWithPostfix)
                .addAllLanes(builders.values().stream().map(ImmutableLane.Builder::build).collect(Collectors.toList()))
                .build();
    }
}
