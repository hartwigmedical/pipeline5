package com.hartwig.pipeline.metadata;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class LocalSampleMetadataApiTest {

    @Test
    public void usesRunIdAndSampleNameAsSetName() {
        LocalSampleMetadataApi victim = new LocalSampleMetadataApi("CPCT1234678R");
        assertThat(victim.get().sampleId()).isEqualTo("CPCT1234678R");
        assertThat(victim.get().sampleName()).isEqualTo("CPCT1234678R");
        assertThat(victim.get().type()).isEqualTo(SingleSampleRunMetadata.SampleType.REFERENCE);
    }
}