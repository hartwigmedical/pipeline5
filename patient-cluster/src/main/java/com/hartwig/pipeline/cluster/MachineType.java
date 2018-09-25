package com.hartwig.pipeline.cluster;

import java.util.NavigableMap;
import java.util.TreeMap;

import org.immutables.value.Value;
import org.jetbrains.annotations.NotNull;

@Value.Immutable
public interface MachineType {

    int MAX_CPUS = 62;

    enum Google {
        STANDARD_4("n1-standard-4"),
        STANDARD_8("n1-standard-8"),
        STANDARD_16("n1-standard-16"),
        STANDARD_32("n1-standard-32"),
        STANDARD_64("n1-standard-64");
        private final String uri;

        Google(final String uri) {
            this.uri = uri;
        }

        public String uri() {
            return uri;
        }
    }

    @Value.Parameter
    String uri();

    static MachineType of(String uri) {
        return ImmutableMachineType.of(uri);
    }

    static MachineType masterFrom(PerformanceProfile profile) {
        return from(profile.cpuMaster());
    }

    static MachineType workerFrom(PerformanceProfile profile) {
        return from(profile.cpuPerWorker());
    }

    @NotNull
    static MachineType from(final int cpuCount) {
        NavigableMap<Integer, Google> lookup = new TreeMap<>();
        if (cpuCount > MAX_CPUS) {
            throw new IllegalArgumentException(String.format(
                    "CPU per cluster over [%s] is not supported due to the high cost of those boxes", cpuCount));
        }
        lookup.put(2, Google.STANDARD_4);
        lookup.put(6, Google.STANDARD_8);
        lookup.put(14, Google.STANDARD_16);
        lookup.put(30, Google.STANDARD_32);
        lookup.put(62, Google.STANDARD_64);
        return MachineType.of(lookup.ceilingEntry(cpuCount).getValue().uri);
    }
}
