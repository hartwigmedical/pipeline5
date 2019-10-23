package com.hartwig.pipeline.transfer.sbp;

import java.util.Map;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.CanonicalGrantee;
import com.amazonaws.services.s3.model.Permission;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SbpS3 {
    static final String READERS_ID_ENV = "READER_ACL_IDS";
    private static final String READERS_ACP_ID_ENV = "READER_ACP_ACL_IDS";
    private static final Logger LOGGER = LoggerFactory.getLogger(SbpS3.class);
    private final AmazonS3 s3Client;
    private Map<String, String> environment;

    SbpS3(final AmazonS3 s3Client, final Map<String, String> environment) {
        this.s3Client = s3Client;
        this.environment = environment;
    }

    void setAclsOn(String bucket, String path) {
        AccessControlList objectAcl = s3Client.getObjectAcl(bucket, path);
        grant(READERS_ID_ENV, Permission.Read, objectAcl, bucket, path);
        grant(READERS_ACP_ID_ENV, Permission.ReadAcp, objectAcl, bucket, path);
    }

    private void grant(final String envKey, final Permission permission, final AccessControlList objectAcl, final String bucket,
            final String path) {
        String identifiers = environment.get(envKey);
        LOGGER.debug("Using environment variable [{}] value [{}]", envKey, identifiers);
        if (identifiers != null && !identifiers.trim().isEmpty()) {
            for (String identifier : identifiers.split(",")) {
                if (identifier != null && !identifier.trim().isEmpty()) {
                    LOGGER.debug("S3 granting [{}] for [{}]", permission, identifier);
                    objectAcl.grantPermission(new CanonicalGrantee(identifier), permission);
                    s3Client.setObjectAcl(bucket, path, objectAcl);
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
