package com.hartwig.pipeline.alignment.sample;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.hartwig.patient.ImmutableLane;
import com.hartwig.patient.Lane;

class FastqFiles {

    static List<Lane> toLanes(final List<String> files, final String directory, final String sampleName) {
        Map<String, ImmutableLane.Builder> builders = new HashMap<>();
        for (String filename : files) {
            if (!(filename.endsWith(".fastq") || filename.endsWith(".fastq.gz"))) {
                continue;
            }
            FastqNamingConvention.apply(filename);
            String[] tokens = filename.split("_");
            String laneNumber = tokens[3];
            String flowCellId = tokens[1];
            ImmutableLane.Builder builder =
                    builders.computeIfAbsent(laneNumber + flowCellId, s -> Lane.builder().laneNumber(laneNumber).flowCellId(flowCellId));
            if (tokens[4].equals("R1")) {
                builder.firstOfPairPath(filename);
            } else if (tokens[4].equals("R2")) {
                builder.secondOfPairPath(filename);
            }
            builder.flowCellId(flowCellId);
        }
        return builders.values().stream().map(ImmutableLane.Builder::build).collect(Collectors.toList());
    }
}
