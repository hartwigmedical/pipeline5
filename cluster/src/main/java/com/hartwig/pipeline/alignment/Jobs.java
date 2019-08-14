package com.hartwig.pipeline.alignment;

import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.execution.dataproc.SparkExecutor;
import com.hartwig.pipeline.storage.GoogleStorageStatusCheck;

class Jobs {
    static Job statusCheckGoogleStorage(final SparkExecutor cluster, final ResultsDirectory resultsDirectory) {
        return new Job(cluster, new GoogleStorageStatusCheck(resultsDirectory));
    }
}
