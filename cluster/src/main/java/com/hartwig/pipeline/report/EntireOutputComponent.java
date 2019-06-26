package com.hartwig.pipeline.report;

import java.util.Arrays;
import java.util.List;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.io.ResultsDirectory;
import com.hartwig.pipeline.io.RuntimeBucket;

public class EntireOutputComponent implements ReportComponent {

    private final RuntimeBucket runtimeBucket;
    private final AlignmentPair pair;
    private final String namespace;
    private final ResultsDirectory resultsDirectory;
    private final String pathPrefix;
    private final List<String> exclusions;

    public EntireOutputComponent(final RuntimeBucket runtimeBucket, final AlignmentPair pair, final String namespace,
            final ResultsDirectory resultsDirectory, final String... exclusions) {
        this(runtimeBucket, pair, namespace, "", resultsDirectory, exclusions);
    }

    EntireOutputComponent(final RuntimeBucket runtimeBucket, final AlignmentPair pair, final String namespace, final String pathPrefix,
            final ResultsDirectory resultsDirectory, final String... exclusions) {
        this.runtimeBucket = runtimeBucket;
        this.pair = pair;
        this.namespace = namespace;
        this.resultsDirectory = resultsDirectory;
        this.pathPrefix = pathPrefix;
        this.exclusions = Arrays.asList(exclusions);
    }

    @Override
    public void addToReport(final Storage storage, final Bucket reportBucket, final String setName) {
        Iterable<Blob> blobs = runtimeBucket.list(resultsDirectory.path());
        for (Blob blob : blobs) {
            String filename = parseFileName(blob);
            if (exclusions.stream().noneMatch(exclusion -> blob.getName().contains(exclusion))) {
                runtimeBucket.copyOutOf(blob.getName(),
                        reportBucket.getName(),
                        String.format("%s/%s/%s/%s",
                                setName,
                                pair.reference().sample() + "_" + pair.tumor().sample(),
                                namespace,
                                pathPrefix.isEmpty() ? filename : pathPrefix + "/" + filename));
            }
        }
    }

    private String parseFileName(final Blob blob) {
        String[] split = blob.getName().split("/");
        return split[split.length - 1];
    }
}
