package com.hartwig.patient.input;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.patient.Sample;

import org.junit.Test;

public class SamplesTest {

    @Test
    public void complementOfReferenceIsTumor() {
        Sample sample = Sample.builder("dir", "SAMPLER").type(Sample.Type.REFERENCE).build();
        Sample complement = Samples.complement(sample);
        assertThat(complement.name()).isEqualTo("SAMPLET");
        assertThat(complement.type()).isEqualTo(Sample.Type.TUMOR);
    }

    @Test
    public void complementOfTumorIsReference(){
        Sample sample = Sample.builder("dir", "SAMPLET").type(Sample.Type.TUMOR).build();
        Sample complement = Samples.complement(sample);
        assertThat(complement.name()).isEqualTo("SAMPLER");
        assertThat(complement.type()).isEqualTo(Sample.Type.REFERENCE);
    }
}