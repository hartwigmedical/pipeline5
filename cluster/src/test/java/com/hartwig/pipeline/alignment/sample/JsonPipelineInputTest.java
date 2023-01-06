package com.hartwig.pipeline.alignment.sample;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.input.JsonPipelineInput;
import com.hartwig.pipeline.input.Lane;
import com.hartwig.pipeline.input.Sample;
import com.hartwig.pipeline.testsupport.Resources;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Test;

public class JsonPipelineInputTest {

    @Test(expected = RuntimeException.class)
    public void fileDoesntExistThrowsRuntimeException() {
        JsonPipelineInput victim = new JsonPipelineInput("/does/not/exist.json");
        victim.sample(TestInputs.referenceRunMetadata());
    }

    @Test(expected = RuntimeException.class)
    public void notValidJsonFileThrowsRuntimeException() {
        JsonPipelineInput victim = new JsonPipelineInput(Resources.testResource("json_sample_source/not_valid.json"));
        victim.sample(TestInputs.referenceRunMetadata());
    }

    @Test
    public void simplePairParsedAndReferenceReturned() {
        JsonPipelineInput victim = new JsonPipelineInput(Resources.testResource("json_sample_source/single_lane.json"));
        Sample sample = victim.sample(TestInputs.referenceRunMetadata());
        assertThat(sample.name()).isEqualTo("CPCT12345678R");
        assertThat(sample.lanes()).hasSize(1);
        Lane lane = sample.lanes().get(0);
        assertThat(lane.firstOfPairPath()).isEqualTo("CPCT12345678R_R1.fastq");
        assertThat(lane.secondOfPairPath()).isEqualTo("CPCT12345678R_R2.fastq");
        assertThat(lane.laneNumber()).isEqualTo("1");
    }

    @Test
    public void simplePairParsedAndTumorReturned() {
        JsonPipelineInput victim = new JsonPipelineInput(Resources.testResource("json_sample_source/single_lane.json"));
        Sample sample = victim.sample(TestInputs.tumorRunMetadata());
        assertThat(sample.name()).isEqualTo("CPCT12345678T");
        assertThat(sample.lanes()).hasSize(1);
        Lane lane = sample.lanes().get(0);
        assertThat(lane.firstOfPairPath()).isEqualTo("CPCT12345678T_R1.fastq");
        assertThat(lane.secondOfPairPath()).isEqualTo("CPCT12345678T_R2.fastq");
        assertThat(lane.laneNumber()).isEqualTo("1");
    }
}