package com.hartwig.pipeline.report;

import java.util.function.Predicate;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.io.ResultsDirectory;
import com.hartwig.pipeline.io.RuntimeBucket;

public class EntireOutputComponent implements ReportComponent {

    private final RuntimeBucket runtimeBucket;
    private final Folder folder;
    private final String namespace;
    private final ResultsDirectory resultsDirectory;
    private final String sourceDirectory;
    private final Predicate<String> exclusions;

    public EntireOutputComponent(final RuntimeBucket runtimeBucket, final Folder folder, final String namespace,
            final ResultsDirectory resultsDirectory) {
        this(runtimeBucket, folder, namespace, "", resultsDirectory, str -> false);
    }

    public EntireOutputComponent(final RuntimeBucket runtimeBucket, final Folder folder, final String namespace,
            final ResultsDirectory resultsDirectory, final Predicate<String> exclusions) {
        this(runtimeBucket, folder, namespace, "", resultsDirectory, exclusions);
    }

    public EntireOutputComponent(final RuntimeBucket runtimeBucket, final Folder folder, final String namespace,
            final String sourceDirectory, final ResultsDirectory resultsDirectory, final Predicate<String> exclude) {
        this.runtimeBucket = runtimeBucket;
        this.folder = folder;
        this.namespace = namespace;
        this.resultsDirectory = resultsDirectory;
        this.sourceDirectory = sourceDirectory;
        this.exclusions = exclude;
    }

    @Override
    public void addToReport(final Storage storage, final Bucket reportBucket, final String setName) {
        Iterable<Blob> blobs = runtimeBucket.list(resultsDirectory.path(sourceDirectory));
        for (Blob blob : blobs) {
            String filename = parseFileName(blob);
            if (!exclusions.test(blob.getName())) {
                runtimeBucket.copyOutOf(blob.getName(),
                        reportBucket.getName(),
                        String.format("%s/%s/%s/%s", setName, folder.name(), namespace, filename));
            }
        }
    }

    private String parseFileName(final Blob blob) {
        String[] split = blob.getName().split("/");
        return split[split.length - 1];
    }
}
