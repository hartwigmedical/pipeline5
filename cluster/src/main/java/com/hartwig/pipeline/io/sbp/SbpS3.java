package com.hartwig.pipeline.io.sbp;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.CanonicalGrantee;
import com.amazonaws.services.s3.model.Permission;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SbpS3 {
    private static final String READERS_ID_ENV = "READER_ACL_IDS";
    private static final String READERS_ACP_ID_ENV = "READER_ACP_ACL_IDS";
    private static final Logger LOGGER = LoggerFactory.getLogger(SbpS3.class);
    private final AmazonS3 s3Client;

    SbpS3(final AmazonS3 s3Client) {
        this.s3Client = s3Client;
    }

    void setAclsOn(String bucket, String path) {
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

    void ensureBucketExists(String bucketName) {
        if (!s3Client.doesBucketExistV2(bucketName)) {
            throw new IllegalStateException(String.format(
                    "Output bucket [%s] did not exist in S3. Check that the bucket is correctly named and exists in the target S3 instance.",
                    bucketName));
        }
    }
}
