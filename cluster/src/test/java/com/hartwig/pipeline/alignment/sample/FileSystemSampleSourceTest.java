package com.hartwig.pipeline.alignment.sample;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.patient.Lane;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.testsupport.Resources;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Test;

public class FileSystemSampleSourceTest {

    private static final String PATIENT_DIR = Resources.testResource("patients/");

    @Test
    public void readsSampleOutOfPatientDirectory() {
        SampleSource victim = new FileSystemSampleSource(PATIENT_DIR);
        Sample sample = victim.sample(TestInputs.referenceRunMetadata());
        assertThat(sample).isNotNull();
        assertThat(sample.name()).isEqualTo(TestInputs.referenceSample());
        assertThat(sample.directory()).isEqualTo(PATIENT_DIR);
        assertThat(sample.lanes()).hasSize(1);
        Lane lane = sample.lanes().get(0);
        assertThat(lane.firstOfPairPath()).isEqualTo(PATIENT_DIR + "reference/reference_flowcell_S1_L001_R1_001.fastq.gz");
        assertThat(lane.secondOfPairPath()).isEqualTo(PATIENT_DIR + "reference/reference_flowcell_S1_L001_R2_001.fastq.gz");
        assertThat(lane.flowCellId()).isEqualTo("flowcell");
        assertThat(lane.index()).isEqualTo("S1");
        assertThat(lane.suffix()).isEqualTo("001");
    }
}