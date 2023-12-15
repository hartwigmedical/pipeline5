package com.hartwig.pipeline.output;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.computeengine.storage.RuntimeBucket;

public class StartupScriptComponent implements OutputComponent {
    private final RuntimeBucket runtimeBucket;
    private final String namespace;
    private final Folder folder;

    public StartupScriptComponent(final RuntimeBucket runtimeBucket, final String namespace, final Folder folder) {
        this.runtimeBucket = runtimeBucket;
        this.namespace = namespace;
        this.folder = folder;
    }

    @Override
    public void addToOutput(final Storage storage, final Bucket outputBucket, final String setName) {
        runtimeBucket.copyOutOf("copy_of_startup_script_for_run.sh", outputBucket.getName(),
                String.format("%s/%s%s/%s", setName, folder.name(), namespace, "run.sh"));
    }
}