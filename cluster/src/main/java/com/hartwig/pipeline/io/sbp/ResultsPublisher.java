package com.hartwig.pipeline.io.sbp;

import static java.lang.String.format;

import java.io.File;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.common.collect.Lists;
import com.hartwig.pipeline.io.CloudCopy;
import com.hartwig.pipeline.metadata.SbpRun;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;

import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResultsPublisher {
    private final ResultsDestinationBuilder destinationBuilder;
    private final CloudCopy cloudCopy;
    private final SbpS3Acl sbpS3Acl;
    private final SBPRestApi sbpApi;
    private final Bucket sourceBucket;
    private final static Logger LOGGER = LoggerFactory.getLogger(ResultsPublisher.class);

    public ResultsPublisher(final ResultsDestinationBuilder destinationBuilder, final CloudCopy cloudCopy, final SbpS3Acl sbpS3Acl,
            final SBPRestApi sbpApi, Bucket sourceBucket) {
        this.destinationBuilder = destinationBuilder;
        this.cloudCopy = cloudCopy;
        this.sbpS3Acl = sbpS3Acl;
        this.sbpApi = sbpApi;
        this.sourceBucket = sourceBucket;
    }

    public void publish(SomaticRunMetadata metadata, SbpRun sbpRun, String sbpBucket) {
        LOGGER.info("Starting file transfer from {} to SBP at {}", sourceBucket.getName(), sbpBucket);
        sbpS3Acl.ensureBucketExists(sbpBucket);
        List<Blob> objects = find(sourceBucket, metadata.runName());
        List<SourceDestPair> allFiles = new ArrayList<>();
        for (Blob blob : objects) {
            CloudFile dest = CloudFile.builder().provider("s3").bucket(sbpBucket).path(blob.getName()).build();
            CloudFile source = CloudFile.builder().provider("gs").bucket(sourceBucket.getName()).path(blob.getName()).md5(blob.getMd5())
                    .size(blob.getSize()).build();
            allFiles.add(new SourceDestPair(source, dest));
        }

        for (SourceDestPair pair : allFiles) {
            cloudCopy.copy(toUrl(pair.source), toUrl(pair.dest));
            sbpS3Acl.setOn((pair.dest.bucket()), pair.dest.path());
            SbpFileMetadata metaData = ImmutableSbpFileMetadata.builder().directory(extractDirectoryNameForSbp(pair.dest.path()))
                    .run_id(Integer.parseInt(sbpRun.id()))
                    .filename(new File(pair.dest.path()).getName())
                    .filesize(pair.source.size())
                    .hash(convertMd5ToSbpFormat(pair.source.md5())).build();
            sbpApi.postFile(metaData);
        }
    }

    private String extractDirectoryNameForSbp(String fullDestFilePath) {
        return new File(fullDestFilePath.substring(fullDestFilePath.indexOf("/") + 1, fullDestFilePath.length() - 1)).getParent();
    }

    private String toUrl(CloudFile cloudFile) {
        return format("%s://%s/%s", cloudFile.provider(), cloudFile.bucket(), cloudFile.path());
    }

    private String convertMd5ToSbpFormat(String originalMd5) {
        return new String(Hex.encodeHex(Base64.getDecoder().decode(originalMd5)));
    }

    private class SourceDestPair {
        CloudFile source;
        CloudFile dest;

        SourceDestPair(CloudFile source, CloudFile dest) {
                    this.source = source;
                        this.dest = dest;
                    }
    }

    private List<Blob> find(Bucket bucket, String prefix) {
        return Lists.newArrayList(bucket.list(Storage.BlobListOption.prefix(prefix)).iterateAll());
    }
}
