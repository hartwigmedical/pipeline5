package com.hartwig.pipeline.metadata;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDateTime;

import com.hartwig.pipeline.Arguments;

import org.junit.Test;

public class LocalSampleMetadataApiTest {

    private static final LocalDateTime NOW = LocalDateTime.of(2019, 5, 21, 19, 25, 0);

    @Test
    public void usesRunIdAndSampleNameAsSetName() {
        LocalSampleMetadataApi
                victim = new LocalSampleMetadataApi(Arguments.testDefaultsBuilder().runId("testmetadata").sampleId("sampler").build(),
                NOW);
        assertThat(victim.get().setName()).isEqualTo("sample-testmetadata");
    }

    @Test
    public void usesSampleAndTimestamp() {
        LocalSampleMetadataApi victim = new LocalSampleMetadataApi(Arguments.testDefaultsBuilder().sampleId("sampler").build(), NOW);
        assertThat(victim.get().setName()).isEqualTo("sample-20190521192500");
    }
}