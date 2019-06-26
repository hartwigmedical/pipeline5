package com.hartwig.pipeline.report;

import com.hartwig.pipeline.io.ResultsDirectory;
import com.hartwig.pipeline.io.RuntimeBucket;

public class RunLogComponent extends SingleFileComponent {

    public RunLogComponent(final RuntimeBucket runtimeBucket, final String namespace, final String sampleName,
            final ResultsDirectory resultsDirectory) {
        super(runtimeBucket, namespace, sampleName, "run.log", "working/run.log", resultsDirectory);
    }
}