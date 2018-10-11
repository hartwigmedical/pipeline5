package com.hartwig.pipeline.io;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.CanonicalGrantee;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.Permission;
import com.amazonaws.services.s3.model.S3Object;
import com.hartwig.patient.Sample;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SBPSampleDownload implements SampleDownload {

    private static final String READERS_ID_ENV = "READER_ACL_IDS";
    private static final String READERS_ACP_ID_ENV = "READER_ACP_ACL_IDS";
    private final Logger LOGGER = LoggerFactory.getLogger(SBPSampleDownload.class);
    private final AmazonS3 s3Client;
    private final SBPRestApi sbpRestApi;
    private final int sbpSampleId;
    private final SampleDownload decorated;

    public SBPSampleDownload(final AmazonS3 s3Client, final SBPRestApi sbpRestApi, final int sbpSampleId, final SampleDownload decorated) {
        this.s3Client = s3Client;
        this.sbpRestApi = sbpRestApi;
        this.sbpSampleId = sbpSampleId;
        this.decorated = decorated;
    }

    @Override
    public void run(final Sample sample, final RuntimeBucket runtimeBucket, final StatusCheck.Status status) {
        decorated.run(sample, runtimeBucket, status);
        String directory = SBPS3FileTarget.ROOT_BUCKET + "/" + sample.barcode();
        String bamFile = sample.name() + ".bam";
        S3Object s3Object = s3Client.getObject(directory, bamFile);
        AccessControlList objectAcl = s3Client.getObjectAcl(directory, bamFile);
        grant(READERS_ID_ENV, Permission.Read, objectAcl);
        grant(READERS_ACP_ID_ENV, Permission.ReadAcp, objectAcl);
        s3Client.setObjectAcl(directory, bamFile, objectAcl);

        ObjectMetadata existing = s3Object.getObjectMetadata();

        sbpRestApi.patchBam(sbpSampleId,
                BamMetadata.builder()
                        .bucket(SBPS3FileTarget.ROOT_BUCKET)
                        .directory(sample.barcode())
                        .filename(bamFile)
                        .filesize(existing.getContentLength())
                        .hash(existing.getETag())
                        .status(status == StatusCheck.Status.SUCCESS ? "Done_PipelineV5" : "Failed_PipelineV5")
                        .build());
    }

    private void grant(final String env, final Permission permission, final AccessControlList objectAcl) {
        String identifiers = System.getenv(env);
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