package com.hartwig.pipeline.metadata;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDateTime;

import com.hartwig.pipeline.Arguments;

import org.junit.Test;

public class PatientMetadataApiTest {

    private static final LocalDateTime NOW = LocalDateTime.of(2019, 5, 21, 19, 25, 0);

    @Test
    public void usesRunIdAndSampleNameAsSetName() {
        PatientMetadataApi victim = new PatientMetadataApi(Arguments.testDefaultsBuilder().runId("testmetadata").sampleId("sampler").build(),
                NOW);
        assertThat(victim.getMetadata().setName()).isEqualTo("sample-testmetadata");
    }

    @Test
    public void usesSampleAndTimestamp() {
        PatientMetadataApi victim = new PatientMetadataApi(Arguments.testDefaultsBuilder().sampleId("sampler").build(), NOW);
        assertThat(victim.getMetadata().setName()).isEqualTo("sample-20190521192500");
    }
}