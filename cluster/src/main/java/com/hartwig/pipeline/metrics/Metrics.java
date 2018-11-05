package com.hartwig.pipeline.metrics;

import java.text.NumberFormat;

import com.hartwig.pipeline.cost.CostCalculator;
import com.hartwig.pipeline.performance.PerformanceProfile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Metrics {

    private static final Logger LOGGER = LoggerFactory.getLogger(Metrics.class);

    static final String BOOTSTRAP = "BOOTSTRAP";
    static final String COST = "COST";
    static final String COST_PER_GB = "COST_PER_GB";
    static final String FASTQ_SIZE_GB = "FASTQ_SIZE_GB";

    private final Monitor monitor;
    private final CostCalculator costCalculator;

    public Metrics(final Monitor monitor, final CostCalculator costCalculator) {
        this.monitor = monitor;
        this.costCalculator = costCalculator;
    }

    void record(String prefix, PerformanceProfile profile, long runtimeMillis) {
        double runtimeHours = runtimeMillis / 1000.0 / 60.0 / 60.0;
        monitor.update(Metric.spentTime(BOOTSTRAP + "_" + prefix, runtimeMillis));
        double cost = costCalculator.calculate(profile, runtimeHours);
        monitor.update(Metric.of(COST, cost));
        LOGGER.info("Stage [{}] completed in [{}] hours for a total cost of [{}]",
                prefix, NumberFormat.getNumberInstance().format(runtimeHours),
                NumberFormat.getCurrencyInstance().format(cost));
        if (profile.fastQSizeGB().isPresent()) {
            monitor.update(Metric.of(FASTQ_SIZE_GB, profile.fastQSizeGB().get()));
            monitor.update(Metric.of(COST_PER_GB, cost / profile.fastQSizeGB().get()));
        }
    }
}
