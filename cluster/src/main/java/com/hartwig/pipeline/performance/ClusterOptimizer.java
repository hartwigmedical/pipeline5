package com.hartwig.pipeline.performance;

import com.hartwig.patient.Sample;
import com.hartwig.pipeline.io.sources.SampleData;

public class ClusterOptimizer {

    private static final long BYTES_PER_GB = (long) Math.pow(1024, 3);

    private final CpuFastQSizeRatio cpuToFastQSizeRatio;
    private final boolean noPreemtibleVms;

    public ClusterOptimizer(final CpuFastQSizeRatio cpuToFastQSizeRatio, final boolean usePreemtibleVms) {
        this.cpuToFastQSizeRatio = cpuToFastQSizeRatio;
        this.noPreemtibleVms = usePreemtibleVms;
    }

    public PerformanceProfile optimize(SampleData sampleData) {
        Sample sample = sampleData.sample();
        if (sampleData.sizeInBytesGZipped() <= 0) {
            throw new IllegalArgumentException(String.format("Sample [%s] lanes had no data. Cannot calculate data size or cpu requirements",
                    sample.name()));
        }
        long totalFileSizeGB = sampleData.sizeInBytesGZipped() / BYTES_PER_GB;
        double totalCpusRequired = totalFileSizeGB * cpuToFastQSizeRatio.cpusPerGB();
        MachineType defaultWorker = MachineType.defaultWorker();
        int numWorkers = new Double(totalCpusRequired / defaultWorker.cpus()).intValue();
        int numPreemptible = noPreemtibleVms ? 0 : numWorkers / 2;
        return PerformanceProfile.builder()
                .master(MachineType.defaultMaster())
                .primaryWorkers(defaultWorker)
                .preemtibleWorkers(MachineType.defaultPreemtibleWorker())
                .numPrimaryWorkers(Math.max(2, numWorkers - numPreemptible))
                .numPreemtibleWorkers(numPreemptible)
                .fastQSizeGB(totalFileSizeGB)
                .build();
    }
}
