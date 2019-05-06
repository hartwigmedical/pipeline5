package com.hartwig.pipeline.cost;

import java.text.NumberFormat;

import com.hartwig.pipeline.execution.dataproc.DataprocPerformanceProfile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataprocCost implements Cost {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataprocCost.class);
    private static final double DATAPROC_PREMIUM = 0.010;
    private final double costPerCpuPerHour;

    private DataprocCost(final double costPerCpuPerHour) {
        this.costPerCpuPerHour = costPerCpuPerHour;
    }

    @Override
    public double calculate(final DataprocPerformanceProfile performanceProfile, final double hours) {
        int totalCpus =
                performanceProfile.master().cpus() + (performanceProfile.primaryWorkers().cpus() * performanceProfile.numPrimaryWorkers())
                        + (performanceProfile.preemtibleWorkers().cpus() * performanceProfile.numPreemtibleWorkers());
        double totalCost = totalCpus * hours * costPerCpuPerHour;
        LOGGER.debug("[{} Dataproc] CPUs for [{}] hours at a cost defaultDirectory [{}] per hour/per cpu for a total cost defaultDirectory [{}]",
                totalCpus,
                hours, NumberFormat.getCurrencyInstance().format(costPerCpuPerHour), NumberFormat.getCurrencyInstance().format(totalCost));
        return totalCost;
    }

    static DataprocCost create() {
        return new DataprocCost(DATAPROC_PREMIUM);
    }
}
