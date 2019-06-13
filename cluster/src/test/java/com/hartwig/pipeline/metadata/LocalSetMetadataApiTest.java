package com.hartwig.pipeline.metadata;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.patient.Sample;

import org.junit.Before;
import org.junit.Test;

public class LocalSetMetadataApiTest {

    private SetMetadataApi victim;

    @Before
    public void setUp() throws Exception {
        victim = new LocalSetMetadataApi("CPCT12345678");
    }

    @Test
    public void impliesTumorSampleNameFromSetName() {
        SetMetadata setMetadata = victim.get();
        assertThat(setMetadata.tumor().type()).isEqualTo(Sample.Type.TUMOR);
        assertThat(setMetadata.tumor().name()).isEqualTo("CPCT12345678T");
    }

    @Test
    public void impliesReferenceSampleNameFromSetName() {
        SetMetadata setMetadata = victim.get();
        assertThat(setMetadata.reference().type()).isEqualTo(Sample.Type.REFERENCE);
        assertThat(setMetadata.reference().name()).isEqualTo("CPCT12345678R");
    }
}