package com.hartwig.bcl2fastq.forensics;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.List;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;

public class ForensicArchive {

    private final Storage storage;
    private final String targetBucket;

    public ForensicArchive(final Storage storage, final String targetBucket) {
        this.storage = storage;
        this.targetBucket = targetBucket;
    }

    public void store(String conversionName, List<Blob> blobsToArchive, String logName) {
        for (Blob blob : blobsToArchive) {
            String[] pathSplit = blob.getName().split("/");
            String filename = pathSplit[pathSplit.length - 1];
            Storage.CopyRequest copyRequest = Storage.CopyRequest.newBuilder()
                    .setSource(BlobId.of(blob.getBucket(), blob.getName()))
                    .setTarget(BlobInfo.newBuilder(targetBucket, conversionName + "/" + filename).build())
                    .build();
            storage.copy(copyRequest).getResult();
        }
        try {
            storage.get(targetBucket).create(conversionName + "/conversion.log", new FileInputStream(logName));
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
