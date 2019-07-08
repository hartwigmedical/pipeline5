package com.hartwig.pipeline.transfer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AccessControlList;

import org.junit.Before;
import org.junit.Test;

public class SbpS3Test {
    private static final String BUCKET = "bucket";
    private static final String PATH_TO_FILE = "path/to/file";
    private String bucket = "some_bucket";
    private AmazonS3 s3;
    private SbpS3 sbp;
    private Map<String, String> environment = new HashMap<>();

    @Before
    public void setup() {
        s3 = mock(AmazonS3.class);
        sbp = new SbpS3(s3, environment);
    }

    @Test
    public void shouldThrowIfBucketDoesNotExist() {
        when(s3.doesBucketExistV2(bucket)).thenReturn(false);
        try {
            sbp.ensureBucketExists(bucket);
        } catch (IllegalStateException ise) {
            // what we want
        }
    }

    @Test
    public void shouldNotThrowIfBucketExists() {
        when(s3.doesBucketExistV2(bucket)).thenReturn(true);
        sbp.ensureBucketExists(bucket);
        verify(s3).doesBucketExistV2(bucket);
        verifyNoMoreInteractions(s3);
    }

    @Test
    public void updatesObjectAclInS3() {
        AccessControlList acl = new AccessControlList();
        environment.put(SbpS3.READERS_ID_ENV, "id");
        when(s3.getObjectAcl(BUCKET, PATH_TO_FILE)).thenReturn(acl);
        sbp.setAclsOn(BUCKET, PATH_TO_FILE);
        verify(s3, times(1)).setObjectAcl(BUCKET, PATH_TO_FILE, acl);
    }
}