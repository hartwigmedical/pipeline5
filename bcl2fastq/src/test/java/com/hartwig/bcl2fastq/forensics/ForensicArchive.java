package com.hartwig.bcl2fastq.forensics;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;

public class ForensicArchive {

    private final Storage storage;
    private final String targetBucket;
    private final String conversionName;

    public ForensicArchive(final Storage storage, final String targetBucket, final String conversionName) {
        this.storage = storage;
        this.targetBucket = targetBucket;
        this.conversionName = conversionName;
    }

    void store(String... pathsToArchive) {
        for (String path : pathsToArchive) {
            String[] pathSplit = path.split("/");
            String sourceBucket = pathSplit[0];
            String subPath = path.substring(sourceBucket.length() + 1);
            Storage.CopyRequest copyRequest = Storage.CopyRequest.newBuilder()
                    .setSource(BlobId.of(sourceBucket, subPath))
                    .setTarget(BlobInfo.newBuilder(targetBucket, conversionName + "/" + subPath).build())
                    .build();
            storage.copy(copyRequest).getResult();
        }
    }
}
