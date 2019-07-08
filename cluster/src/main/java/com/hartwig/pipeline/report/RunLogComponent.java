package com.hartwig.pipeline.report;

import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.storage.RuntimeBucket;

public class RunLogComponent extends SingleFileComponent {

    public RunLogComponent(final RuntimeBucket runtimeBucket, final String namespace, final Folder folder,
            final ResultsDirectory resultsDirectory) {
        super(runtimeBucket, namespace, folder, "run.log", "run.log", resultsDirectory);
    }
}