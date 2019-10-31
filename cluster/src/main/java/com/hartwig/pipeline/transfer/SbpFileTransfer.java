package com.hartwig.pipeline.transfer;

import static java.lang.String.format;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.common.collect.Lists;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.report.PipelineResults;
import com.hartwig.pipeline.sbpapi.SbpFileMetadata;
import com.hartwig.pipeline.sbpapi.SbpRestApi;
import com.hartwig.pipeline.sbpapi.SbpRun;
import com.hartwig.pipeline.storage.CloudCopy;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SbpFileTransfer {
    private final CloudCopy cloudCopy;
    private final SbpS3 sbpS3;
    private final SbpRestApi sbpApi;
    private final Bucket sourceBucket;
    private final ContentTypeCorrection contentTypeCorrection;
    private final Arguments arguments;
    private final static Logger LOGGER = LoggerFactory.getLogger(SbpFileTransfer.class);
    public final static String MANIFEST_FILENAME = "MANIFEST";

    SbpFileTransfer(final CloudCopy cloudCopy, final SbpS3 sbpS3, final SbpRestApi sbpApi, Bucket sourceBucket,
            ContentTypeCorrection contentTypeCorrection, final Arguments arguments) {
        this.cloudCopy = cloudCopy;
        this.sbpS3 = sbpS3;
        this.sbpApi = sbpApi;
        this.sourceBucket = sourceBucket;
        this.contentTypeCorrection = contentTypeCorrection;
        this.arguments = arguments;
    }

    public void publish(SomaticRunMetadata metadata, SbpRun sbpRun, String sbpBucket) {
        LOGGER.info("Starting file transfer from {} to SBP bucket {}", sourceBucket.getName(), sbpBucket);
        sbpS3.ensureBucketExists(sbpBucket);
        List<Blob> sourceObjects = find(sourceBucket, metadata.runName());
        List<SourceDestPair> allFiles = new ArrayList<>();
        for (Blob blob : filterStagingBlobs(sourceObjects)) {
            if (blob.getMd5() == null) {
                throw new IllegalStateException(format("Object gs://%s/%s has a null MD5", sourceBucket.getName(), blob.getName()));
            } else {
                contentTypeCorrection.apply(blob);
                allFiles.add(createPair(blob, sbpBucket));
            }
        }
        writeManifest(allFiles, sbpBucket, metadata.runName(), sbpRun);
        for (SourceDestPair pair : allFiles) {
            LOGGER.debug("Copying {}", pair);
            doRemoteWork(pair.source.toUrl(), Integer.parseInt(sbpRun.id()), pair.source.size(), pair.source.md5(), pair.dest);
        }
        if (arguments.cleanup()) {
            sourceObjects.forEach(Blob::delete);
        }
    }

    private SourceDestPair createPair(Blob blob, String sbpBucket) {
        CloudFile dest =
                CloudFile.builder().provider("s3").bucket(sbpBucket).path(blob.getName()).size(blob.getSize()).md5(blob.getMd5()).build();
        CloudFile source = CloudFile.builder()
                .provider("gs")
                .bucket(sourceBucket.getName())
                .path(blob.getName())
                .md5(blob.getMd5())
                .size(blob.getSize())
                .build();
        return new SourceDestPair(source, dest);
    }

    private void writeManifest(List<SourceDestPair> allFiles, String sbpBucket, String directory, SbpRun sbpRun) {
        LOGGER.debug("Generating manifest");
        String manifestContents =
                allFiles.stream().map(SourceDestPair::getSource).map(CloudFile::toManifestForm).collect(Collectors.joining("\n"));

        String manifestKey = directory + "/" + MANIFEST_FILENAME;

        sbpS3.createFile(sbpBucket, manifestKey, new ByteArrayInputStream(manifestContents.getBytes()));
        SbpFileMetadata metaData = SbpFileMetadata.builder()
                .directory(directory)
                .run_id(Integer.parseInt(sbpRun.id()))
                .filename(MANIFEST_FILENAME)
                .filesize(manifestContents.getBytes().length)
                .hash(DigestUtils.md5Hex(manifestContents.getBytes()))
                .build();
        sbpApi.postFile(metaData);
    }

    private void doRemoteWork(String sourceUrl, int runId, long filesize, String md5, CloudFile dest) {
        cloudCopy.copy(sourceUrl, dest.toUrl());
        sbpS3.setAclsOn((dest.bucket()), dest.path());
        SbpFileMetadata metaData = SbpFileMetadata.builder()
                .directory(extractDirectoryNameForSbp(dest.path()))
                .run_id(runId)
                .filename(new File(dest.path()).getName())
                .filesize(filesize)
                .hash(convertMd5ToSbpFormat(md5))
                .build();
        sbpApi.postFile(metaData);
    }

    private List<Blob> filterStagingBlobs(final List<Blob> sourceObjects) {
        return sourceObjects.stream()
                .filter(blob -> !blob.getName().endsWith(PipelineResults.STAGING_COMPLETE))
                .collect(Collectors.toList());
    }

    private String extractDirectoryNameForSbp(String fullDestFilePath) {
        String parent = new File(fullDestFilePath.substring(fullDestFilePath.indexOf("/") + 1, fullDestFilePath.length() - 1)).getParent();
        return parent != null ? parent : "";
    }

    private String convertMd5ToSbpFormat(String originalMd5) {
        return new String(Hex.encodeHex(Base64.getDecoder().decode(originalMd5)));
    }

    private static class SourceDestPair {
        CloudFile source;
        CloudFile dest;

        SourceDestPair(CloudFile source, CloudFile dest) {
            this.source = source;
            this.dest = dest;
        }

        public CloudFile getSource() {
            return source;
        }

        public CloudFile getDest() {
            return dest;
        }

        @Override
        public String toString() {
            return format("%s -> %s", source.toUrl(), dest.toUrl());
        }
    }

    private List<Blob> find(Bucket bucket, String prefix) {
        return Lists.newArrayList(bucket.list(Storage.BlobListOption.prefix(prefix + "/")).iterateAll());
    }
}
