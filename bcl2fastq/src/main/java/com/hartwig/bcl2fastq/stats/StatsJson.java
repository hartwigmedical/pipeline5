package com.hartwig.bcl2fastq.stats;

import java.io.IOException;

import com.hartwig.pipeline.jackson.ObjectMappers;

public class StatsJson {

    private final Stats stats;

    public StatsJson(final String statsJson) {
        try {
            stats = ObjectMappers.get().readValue(statsJson, Stats.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Stats stats() {
        return stats;
    }
}
