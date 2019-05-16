package com.hartwig.pipeline.alignment;

import com.hartwig.pipeline.execution.dataproc.SparkExecutor;
import com.hartwig.pipeline.io.GoogleStorageStatusCheck;
import com.hartwig.pipeline.io.ResultsDirectory;
import com.hartwig.pipeline.io.StatusCheck;

class Jobs {

    static Job noStatusCheck(final SparkExecutor cluster) {
        return new Job(cluster, StatusCheck.alwaysSuccess());
    }

    static Job statusCheckGoogleStorage(final SparkExecutor cluster, final ResultsDirectory resultsDirectory) {
        return new Job(cluster, new GoogleStorageStatusCheck(resultsDirectory));
    }
}
