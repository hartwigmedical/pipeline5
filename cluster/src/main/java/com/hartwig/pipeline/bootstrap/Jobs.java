package com.hartwig.pipeline.bootstrap;

import com.hartwig.pipeline.cluster.SparkExecutor;
import com.hartwig.pipeline.cost.CostCalculator;
import com.hartwig.pipeline.io.GoogleStorageStatusCheck;
import com.hartwig.pipeline.io.ResultsDirectory;
import com.hartwig.pipeline.io.StatusCheck;
import com.hartwig.pipeline.metrics.Monitor;
import com.hartwig.pipeline.performance.PerformanceProfile;

class Jobs {

    static Job noStatusCheck(final SparkExecutor cluster, final CostCalculator costCalculator, final Monitor monitor) {
        return new Job(cluster,
                costCalculator,
                monitor,
                StatusCheck.alwaysSuccess());
    }

    static Job statusCheckGoogleStorage(final SparkExecutor cluster, final CostCalculator costCalculator, final Monitor monitor) {
        return new Job(cluster,
                costCalculator,
                monitor,
                new GoogleStorageStatusCheck(ResultsDirectory.defaultDirectory()));
    }
}
