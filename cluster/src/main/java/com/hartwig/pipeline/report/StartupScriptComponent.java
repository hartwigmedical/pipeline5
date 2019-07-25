package com.hartwig.pipeline.report;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.storage.RuntimeBucket;

public class StartupScriptComponent implements ReportComponent {
    private final RuntimeBucket runtimeBucket;
    private final String namespace;
    private final Folder folder;

    public StartupScriptComponent(final RuntimeBucket runtimeBucket, final String namespace, final Folder folder) {
        this.runtimeBucket = runtimeBucket;
        this.namespace = namespace;
        this.folder = folder;
    }

    @Override
    public void addToReport(final Storage storage, final Bucket reportBucket, final String setName) {
        runtimeBucket.copyOutOf("copy_of_startup_script_used_for_this_run.sh", reportBucket.getName(),
                String.format("%s/%s%s/%s", setName, folder.name(), namespace, "run.sh"));
    }
}