package com.hartwig.pipeline.io.sbp;

import java.util.Base64;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.CanonicalGrantee;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.Permission;
import com.amazonaws.services.s3.model.S3Object;
import com.google.cloud.storage.Blob;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.alignment.JobResult;
import com.hartwig.pipeline.io.BamDownload;
import com.hartwig.pipeline.io.BamNames;
import com.hartwig.pipeline.io.ResultsDirectory;
import com.hartwig.pipeline.io.RuntimeBucket;

import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SBPSampleMetadataPatch implements BamDownload {

    static final String READERS_ID_ENV = "READER_ACL_IDS";
    static final String READERS_ACP_ID_ENV = "READER_ACP_ACL_IDS";
    private final Logger LOGGER = LoggerFactory.getLogger(SBPSampleMetadataPatch.class);
    private final AmazonS3 s3Client;
    private final SBPRestApi sbpRestApi;
    private final int sbpSampleId;
    private final BamDownload decorated;
    private final ResultsDirectory resultsDirectory;
    private final EnvironmentVariables environmentVariables;

    public SBPSampleMetadataPatch(final AmazonS3 s3Client, final SBPRestApi sbpRestApi, final int sbpSampleId, final BamDownload decorated,
            final ResultsDirectory resultsDirectory, final EnvironmentVariables environmentVariables) {
        this.s3Client = s3Client;
        this.sbpRestApi = sbpRestApi;
        this.sbpSampleId = sbpSampleId;
        this.decorated = decorated;
        this.resultsDirectory = resultsDirectory;
        this.environmentVariables = environmentVariables;
    }

    @Override
    public void run(final Sample sample, final RuntimeBucket runtimeBucket, final JobResult result) {
        Blob bamBlob = runtimeBucket.bucket().get(resultsDirectory.path(BamNames.sorted(sample)));
        decorated.run(sample, runtimeBucket, result);
        String bamFile = sample.name() + ".bam";
        String baiFile = bamFile + ".bai";
        S3Object s3Object = setACLs(sample, bamFile);
        setACLs(sample, baiFile);
        ObjectMetadata existing = s3Object.getObjectMetadata();
        sbpRestApi.patchBam(sbpSampleId,
                BamMetadata.builder()
                        .bucket(SBPS3FileTarget.ROOT_BUCKET)
                        .directory(sample.barcode())
                        .filename(bamFile)
                        .filesize(existing.getContentLength())
                        .hash(new String(Hex.encodeHex(Base64.getDecoder().decode(bamBlob.getMd5()))))
                        .status(result == JobResult.SUCCESS ? "Done_PipelineV5" : "Failed_PipelineV5")
                        .build());
    }

    private S3Object setACLs(final Sample sample, final String bamFile) {
        String key = sample.barcode() + "/" + bamFile;
        S3Object s3Object = s3Client.getObject(SBPS3FileTarget.ROOT_BUCKET, key);
        AccessControlList objectAcl = s3Client.getObjectAcl(SBPS3FileTarget.ROOT_BUCKET, key);
        grant(READERS_ID_ENV, Permission.Read, objectAcl);
        grant(READERS_ACP_ID_ENV, Permission.ReadAcp, objectAcl);
        s3Client.setObjectAcl(SBPS3FileTarget.ROOT_BUCKET, key, objectAcl);
        return s3Object;
    }

    private void grant(final String env, final Permission permission, final AccessControlList objectAcl) {
        String identifiers = environmentVariables.get(env);
        LOGGER.info("Value of environment variable [{}] was [{}]", env, identifiers);
        if (identifiers != null && !identifiers.trim().isEmpty()) {
            for (String identifier : identifiers.split(",")) {
                if (identifier != null && !identifier.trim().isEmpty()) {
                    LOGGER.info("S3 granting [{}] for [{}]", permission, identifier);
                    objectAcl.grantPermission(new CanonicalGrantee(identifier), permission);
                }
            }
        }
    }
}