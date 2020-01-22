package com.hartwig.bcl2fastq;

import java.util.Comparator;
import java.util.stream.StreamSupport;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;

public class InputPath {

    static String resolve(final Bucket bucket, final String flowcellId) {
        return StreamSupport.stream(bucket.list().iterateAll().spliterator(), false)
                .sorted(Comparator.comparingLong(BlobInfo::getCreateTime).reversed())
                .map(Blob::getName)
                .filter(n -> n.endsWith(flowcellId))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(String.format("No paths found in input bucket for flowcell [%s]",
                        flowcellId)));
    }
}
