package com.hartwig.bcl2fastq;

import java.util.Comparator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;

public class InputPath {

    static String resolve(final Bucket bucket, final String flowcellId) {
        Stream<String> stringStream = StreamSupport.stream(bucket.list().iterateAll().spliterator(), false)
                .sorted(Comparator.comparingLong(BlobInfo::getCreateTime).reversed())
                .map(Blob::getName)
                .map(s -> s.split("/")[0])
                .filter(n -> n.endsWith(flowcellId));
        return stringStream.findFirst()
                .orElseThrow(() -> new IllegalArgumentException(String.format("No paths found in input bucket for flowcell [%s]",
                        flowcellId)));
    }
}
