package com.hartwig.pipeline.cost;

import com.hartwig.pipeline.performance.PerformanceProfile;

public interface Cost {
    double calculate(PerformanceProfile performanceProfile, double hours);
}
