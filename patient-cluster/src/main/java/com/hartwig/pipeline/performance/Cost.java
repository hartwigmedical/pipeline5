package com.hartwig.pipeline.performance;

public class Cost {

    private final double HOUR_ESTIMATE = 4.0;

    public double calculate(PerformanceProfile profile) {
        final double dataprocOverhead = 3.20;
        final double disk = 0.66;
        return masterCost(profile) + workerCost(profile.primaryWorkers(), profile.numPrimaryWorkers())
                + workerCost(profile.preemtibleWorkers(), profile.numPreemtibleWorkers()) + dataprocOverhead + disk;
    }

    private double workerCost(final MachineType machineType, final int workers) {
        return machineType.costPerInstancePerHour() * workers * HOUR_ESTIMATE;
    }

    private double masterCost(final PerformanceProfile profile) {
        return profile.master().costPerInstancePerHour() * HOUR_ESTIMATE;
    }
}
