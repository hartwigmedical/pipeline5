package com.hartwig.pipeline.performance;

import java.util.function.ToDoubleFunction;
import java.util.stream.Stream;

import com.hartwig.patient.Sample;

public class ClusterOptimizer {

    private static final int FIXED_PRIMARY_WORKERS = 2;
    private final CpuFastQSizeRatio cpuToFastQSizeRatio;
    private final ToDoubleFunction<String> fileSizeCalculator;

    public ClusterOptimizer(final CpuFastQSizeRatio cpuToFastQSizeRatio, final ToDoubleFunction<String> fileSizeCalculator) {
        this.cpuToFastQSizeRatio = cpuToFastQSizeRatio;
        this.fileSizeCalculator = fileSizeCalculator;
    }

    public PerformanceProfile optimize(Sample sample) {
        if (sample.lanes().isEmpty()) {
            throw new IllegalArgumentException(String.format("No lanes in sample [%s]. Cannot calculate data size or cpu requirements",
                    sample.name()));
        }
        double totalFileSizeGB = sample.lanes()
                .stream()
                .flatMap(lane -> Stream.of(lane.readsPath(), lane.matesPath()))
                .mapToDouble(fileSizeCalculator)
                .sum();
        if (totalFileSizeGB <= 0) {
            throw new IllegalArgumentException(String.format("Sample [%s] lanes had no data. Cannot calculate data size or cpu requirements",
                    sample.name()));
        }
        double totalCpusRequired = totalFileSizeGB * cpuToFastQSizeRatio.cpusPerGB();
        MachineType defaultWorker = MachineType.defaultWorker();
        int numWorkers = new Double(totalCpusRequired / defaultWorker.cpus()).intValue();
        return PerformanceProfile.builder()
                .master(MachineType.defaultMaster())
                .primaryWorkers(defaultWorker)
                .preemtibleWorkers(MachineType.defaultPreemtibleWorker())
                .numPrimaryWorkers(FIXED_PRIMARY_WORKERS)
                .numPreemtibleWorkers(numWorkers > 2 ? numWorkers - FIXED_PRIMARY_WORKERS : 0)
                .build();
    }
}
