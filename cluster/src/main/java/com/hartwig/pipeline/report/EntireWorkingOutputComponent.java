package com.hartwig.pipeline.report;

import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.io.ResultsDirectory;
import com.hartwig.pipeline.io.RuntimeBucket;

public class EntireWorkingOutputComponent extends EntireOutputComponent {

    public EntireWorkingOutputComponent(final RuntimeBucket runtimeBucket, final AlignmentPair pair, final String namespace,
            final ResultsDirectory resultsDirectory, final String... exclusions) {
        super(runtimeBucket, pair, namespace, "working", resultsDirectory, exclusions);
    }
}
