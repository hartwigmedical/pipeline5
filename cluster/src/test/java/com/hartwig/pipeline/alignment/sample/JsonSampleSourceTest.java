package com.hartwig.pipeline.alignment.sample;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.patient.Lane;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.testsupport.Resources;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Test;

public class JsonSampleSourceTest {

    @Test(expected = RuntimeException.class)
    public void fileDoesntExistThrowsRuntimeException() {
        JsonSampleSource victim = new JsonSampleSource("/does/not/exist.json");
        victim.sample(TestInputs.referenceRunMetadata());
    }

    @Test(expected = RuntimeException.class)
    public void notValidJsonFileThrowsRuntimeException() {
        JsonSampleSource victim = new JsonSampleSource(Resources.testResource("json_sample_source/not_valid.json"));
        victim.sample(TestInputs.referenceRunMetadata());
    }

    @Test
    public void simplePairParsedAndReferenceReturned() {
        JsonSampleSource victim = new JsonSampleSource(Resources.testResource("json_sample_source/single_lane.json"));
        Sample sample = victim.sample(TestInputs.referenceRunMetadata());
        assertThat(sample.type()).isEqualTo(Sample.Type.REFERENCE);
        assertThat(sample.name()).isEqualTo("CPCT12345678R");
        assertThat(sample.lanes()).hasSize(1);
        Lane lane = sample.lanes().get(0);
        assertThat(lane.firstOfPairPath()).isEqualTo("CPCT12345678R_R1.fastq");
        assertThat(lane.secondOfPairPath()).isEqualTo("CPCT12345678R_R2.fastq");
        assertThat(lane.laneNumber()).isEqualTo("1");
    }

    @Test
    public void simplePairParsedAndTumorReturned() {
        JsonSampleSource victim = new JsonSampleSource(Resources.testResource("json_sample_source/single_lane.json"));
        Sample sample = victim.sample(TestInputs.tumorRunMetadata());
        assertThat(sample.type()).isEqualTo(Sample.Type.TUMOR);
        assertThat(sample.name()).isEqualTo("CPCT12345678T");
        assertThat(sample.lanes()).hasSize(1);
        Lane lane = sample.lanes().get(0);
        assertThat(lane.firstOfPairPath()).isEqualTo("CPCT12345678T_R1.fastq");
        assertThat(lane.secondOfPairPath()).isEqualTo("CPCT12345678T_R2.fastq");
        assertThat(lane.laneNumber()).isEqualTo("1");
    }
}