package com.hartwig.pipeline.cost;

import com.hartwig.pipeline.execution.dataproc.DataprocPerformanceProfile;

public interface Cost {
    double calculate(DataprocPerformanceProfile performanceProfile, double hours);
}
