package com.hartwig.pipeline.metadata;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.Arguments;

import org.junit.Before;
import org.junit.Test;

public class LocalSomaticMetadataApiTest {

    private SomaticMetadataApi victim;

    @Before
    public void setUp() throws Exception {
        victim = new LocalSomaticMetadataApi(Arguments.testDefaultsBuilder().setId("CPCT12345678").build());
    }

    @Test
    public void impliestumorFromSetName() {
        SomaticRunMetadata setMetadata = victim.get();
        assertThat(setMetadata.tumor().sampleName()).isEqualTo("CPCT12345678T");
    }

    @Test
    public void impliesReferenceSampleNameFromSetName() {
        SomaticRunMetadata setMetadata = victim.get();
        assertThat(setMetadata.reference().sampleName()).isEqualTo("CPCT12345678R");
    }
}