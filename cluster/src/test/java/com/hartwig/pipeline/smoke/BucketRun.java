package com.hartwig.pipeline.smoke;

import static java.lang.String.format;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobListOption;

public class BucketRun {
    private final Storage backingStorage;
    private final String outputBucket;
    private final String runName;

    public BucketRun(final Storage storage, final String outputBucket, final String runName) {
        this.backingStorage = storage;
        this.outputBucket = outputBucket;
        this.runName = runName;
    }

    public File download() {
        try {
            File localDir = Files.createTempDirectory(format("smoketest-%s-", runName)).toFile();
            for (final Blob remote : backingStorage.get(outputBucket).list(BlobListOption.prefix(runName)).iterateAll()) {
                if (!(remote.getName().toLowerCase().endsWith(".bam") || remote.getName().toLowerCase().endsWith(".cram"))) {
                    Path fullLocalPath = Paths.get(localDir.getAbsolutePath(), remote.getName());
                    ensureParentDirAndDownload(remote, localDir.toPath().resolve(fullLocalPath));
                }
            }
            return localDir;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void ensureParentDirAndDownload(final Blob remote, final Path fullLocalPath) {
        fullLocalPath.getParent().toFile().mkdirs();
        remote.downloadTo(fullLocalPath);
    }
}
