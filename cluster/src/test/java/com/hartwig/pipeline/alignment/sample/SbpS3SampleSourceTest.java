package com.hartwig.pipeline.alignment.sample;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;
import com.hartwig.patient.Lane;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class SbpS3SampleSourceTest {

    private static final int SAMPLE_ID = 1;

    @Test
    public void requestMadeToS3ForContentLength() {
        SbpSampleReader sbpSampleReader = mock(SbpSampleReader.class);
        AmazonS3 s3 = mock(AmazonS3.class);
        SbpS3SampleSource victim = new SbpS3SampleSource(s3, sbpSampleReader);

        ArgumentCaptor<String> bucketCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> objectCaptor = ArgumentCaptor.forClass(String.class);
        when(sbpSampleReader.read(SAMPLE_ID)).thenReturn(Sample.builder("", "")
                .addLanes(Lane.builder()
                        .directory("")
                        .name("test")
                        .flowCellId("")
                        .suffix("")
                        .index("1")
                        .laneNumber("L001")
                        .firstOfPairPath("obj02_input/HMF_COLO829/COLO829R/COLO829R_AHCT3FCCXY_S2_L001_R1_001.fastq.gz")
                        .secondOfPairPath("obj02_input/HMF_COLO829/COLO829R/COLO829R_AHCT3FCCXY_S2_L001_R2_001.fastq.gz")
                        .build())
                .build());
        when(s3.getObject(bucketCaptor.capture(), objectCaptor.capture())).thenReturn(new S3Object());

        victim.sample(TestInputs.referenceRunMetadata().withEntityId(SAMPLE_ID));

        assertThat(bucketCaptor.getAllValues().get(0)).isEqualTo("obj02_input");
        assertThat(objectCaptor.getAllValues().get(0)).isEqualTo("HMF_COLO829/COLO829R/COLO829R_AHCT3FCCXY_S2_L001_R1_001.fastq.gz");
        assertThat(bucketCaptor.getAllValues().get(1)).isEqualTo("obj02_input");
        assertThat(objectCaptor.getAllValues().get(1)).isEqualTo("HMF_COLO829/COLO829R/COLO829R_AHCT3FCCXY_S2_L001_R2_001.fastq.gz");
    }
}
