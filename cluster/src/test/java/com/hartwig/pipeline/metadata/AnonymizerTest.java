package com.hartwig.pipeline.metadata;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.api.model.Sample;
import com.hartwig.pipeline.Arguments;

import org.junit.Test;

public class AnonymizerTest {

    private static final String SAMPLE_NAME = "sample";
    private static final String BARCODE = "barcode";
    private static final Sample SAMPLE = new Sample().name(SAMPLE_NAME).barcode(BARCODE);

    @Test
    public void usesSampleNameWhenTurnedOff() {
        Anonymizer victim = new Anonymizer(Arguments.testDefaultsBuilder().anonymize(false).build());
        assertThat(victim.sampleName(SAMPLE)).isEqualTo(SAMPLE_NAME);
    }

    @Test
    public void usesBarcodeNameWhenTurnedOff() {
        Anonymizer victim = new Anonymizer(Arguments.testDefaultsBuilder().anonymize(true).build());
        assertThat(victim.sampleName(SAMPLE)).isEqualTo(BARCODE);
    }
}