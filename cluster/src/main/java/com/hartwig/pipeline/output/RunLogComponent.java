package com.hartwig.pipeline.output;

import com.hartwig.computeengine.storage.ResultsDirectory;
import com.hartwig.computeengine.storage.RuntimeBucket;

public class RunLogComponent extends SingleFileComponent {

    public static final String LOG_FILE = "run.log";

    public RunLogComponent(final RuntimeBucket runtimeBucket, final String namespace, final Folder folder,
            final ResultsDirectory resultsDirectory) {
        super(runtimeBucket, namespace, folder, LOG_FILE, LOG_FILE, resultsDirectory);
    }
}