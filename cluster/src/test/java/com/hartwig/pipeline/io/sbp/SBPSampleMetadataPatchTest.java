package com.hartwig.pipeline.io.sbp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.Grant;
import com.amazonaws.services.s3.model.Permission;
import com.amazonaws.services.s3.model.S3Object;
import com.hartwig.patient.ImmutableSample;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.alignment.AlignmentOutputPaths;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.io.ResultsDirectory;
import com.hartwig.pipeline.testsupport.MockRuntimeBucket;

import org.junit.Before;
import org.junit.Test;

public class SBPSampleMetadataPatchTest {

    private static final String SAMPLE_NAME = "sample";
    private static final String BARCODE = "barcodeOrSampleName";
    private static final String BAM_KEY = BARCODE + "/" + SAMPLE_NAME + ".bam";
    private static final String BAI_KEY = BAM_KEY + ".bai";
    private static final ImmutableSample SAMPLE = Sample.builder("", SAMPLE_NAME).barcode(BARCODE).build();
    private static final String READER_1 = "reader1";
    private static final String READER_2 = "reader2";
    private static final String READERS = READER_1 + "," + READER_2;
    private static final String READER_ACP = "reader_acp";
    private AmazonS3 s3;
    private ResultsDirectory resultsDirectory;
    private SBPSampleMetadataPatch victim;
    private EnvironmentVariables environmentVariables;

    @Before
    public void setUp() throws Exception {
        s3 = mock(AmazonS3.class);
        when(s3.getObject(SBPS3FileTarget.ROOT_BUCKET, BAM_KEY)).thenReturn(new S3Object());
        SBPRestApi sbpRestApi = mock(SBPRestApi.class);
        resultsDirectory = ResultsDirectory.defaultDirectory();
        environmentVariables = mock(EnvironmentVariables.class);
        victim = new SBPSampleMetadataPatch(s3, sbpRestApi, 1, (sample, runtimeBucket, result) -> {
        }, resultsDirectory, environmentVariables);
    }

    @Test
    public void addsAclsToBamAndBaiFile() {
        when(environmentVariables.get(SBPSampleMetadataPatch.READERS_ID_ENV)).thenReturn(READERS);
        when(environmentVariables.get(SBPSampleMetadataPatch.READERS_ACP_ID_ENV)).thenReturn(READER_ACP);

        AccessControlList bamAcl = new AccessControlList();
        when(s3.getObjectAcl(SBPS3FileTarget.ROOT_BUCKET, BAM_KEY)).thenReturn(bamAcl);
        AccessControlList baiAcl = new AccessControlList();
        when(s3.getObjectAcl(SBPS3FileTarget.ROOT_BUCKET, BAI_KEY)).thenReturn(baiAcl);
        MockRuntimeBucket runtimeBucket = MockRuntimeBucket.of("test").with(resultsDirectory.path(AlignmentOutputPaths.sorted(SAMPLE)), 1, "md5");
        victim.run(SAMPLE, runtimeBucket.getRuntimeBucket(), PipelineStatus.SUCCESS);
        checkAcl(bamAcl);
        checkAcl(baiAcl);
    }

    private static void checkAcl(final AccessControlList bamAcl) {
        checkGrant(READER_1, Permission.Read, bamAcl.getGrantsAsList().get(0));
        checkGrant(READER_2, Permission.Read, bamAcl.getGrantsAsList().get(1));
        checkGrant(READER_ACP, Permission.ReadAcp, bamAcl.getGrantsAsList().get(2));
    }

    private static void checkGrant(final String grantee, final Permission permission, final Grant grant) {
        assertThat(grant.getGrantee().getIdentifier()).isEqualTo(grantee);
        assertThat(grant.getPermission()).isEqualTo(permission);
    }
}