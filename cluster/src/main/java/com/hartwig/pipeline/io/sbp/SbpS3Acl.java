package com.hartwig.pipeline.io.sbp;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.CanonicalGrantee;
import com.amazonaws.services.s3.model.Permission;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SbpS3Acl {
    private static final String READERS_ID_ENV = "READER_ACL_IDS";
    private static final String READERS_ACP_ID_ENV = "READER_ACP_ACL_IDS";
    private final Logger LOGGER = LoggerFactory.getLogger(SbpS3Acl.class);
    private final AmazonS3 s3Client;

    public SbpS3Acl(final AmazonS3 s3Client) {
        this.s3Client = s3Client;
    }

    public void setOn(String bucket, String path) {
        AccessControlList objectAcl = s3Client.getObjectAcl(bucket, path);
        grant(READERS_ID_ENV, Permission.Read, objectAcl);
        grant(READERS_ACP_ID_ENV, Permission.ReadAcp, objectAcl);
    }

    private void grant(final String envKey, final Permission permission, final AccessControlList objectAcl) {
        String identifiers = System.getenv().get(envKey);
        LOGGER.info("Using environment variable [{}] value [{}]", envKey, identifiers);
        if (identifiers != null && !identifiers.trim().isEmpty()) {
            for (String identifier : identifiers.split(",")) {
                if (identifier != null && !identifier.trim().isEmpty()) {
                    LOGGER.info("S3 granting [{}] for [{}]", permission, identifier);
                    objectAcl.grantPermission(new CanonicalGrantee(identifier), permission);
                }
            }
        }
    }

    public void ensureBucketExists(String bucketName) {
        if (!s3Client.doesBucketExistV2(bucketName)) {
            s3Client.createBucket(bucketName);
        }
    }
}
