package com.hartwig.pipeline.bootstrap;

import com.hartwig.pipeline.cost.CostCalculator;
import com.hartwig.pipeline.metrics.Metric;
import com.hartwig.pipeline.metrics.Monitor;
import com.hartwig.pipeline.performance.PerformanceProfile;

class Metrics {

    static final String BOOTSTRAP = "BOOTSTRAP";
    static final String COST = "COST";
    static final String COST_PER_GB = "COST_PER_GB";
    static final String FASTQ_SIZE_GB = "FASTQ_SIZE_GB";

    static void record(PerformanceProfile profile, long runtimeMillis, Monitor monitor, CostCalculator costCalculator) {
        double runtimeHours = runtimeMillis / 1000.0 / 60.0 / 60.0;
        monitor.update(Metric.spentTime(BOOTSTRAP, runtimeMillis));
        double cost = costCalculator.calculate(profile, runtimeHours);
        monitor.update(Metric.of(COST, cost));
        if (profile.fastQSizeGB().isPresent()) {
            monitor.update(Metric.of(FASTQ_SIZE_GB, profile.fastQSizeGB().get()));
            monitor.update(Metric.of(COST_PER_GB, cost / profile.fastQSizeGB().get()));
        }
    }
}
