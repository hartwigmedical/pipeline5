package com.hartwig.pipeline.metadata;

import static java.lang.String.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.hartwig.patient.Sample;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.alignment.sample.JsonSampleSource;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata.SampleType;

import org.junit.Before;
import org.junit.Test;

public class LocalSomaticMetadataTest {

    private SomaticMetadataApi victim;
    private String setId;
    private String tumorName;
    private String refName;

    @Before
    public void setUp() throws Exception {
        setId = "setId";
        tumorName = "CORE123T";
        refName = "CORE123R";
        final JsonSampleSource jsonSampleSource = mock(JsonSampleSource.class);
        final Sample tumorSample = mock(Sample.class);
        final Sample refSample = mock(Sample.class);
        victim = new LocalSomaticMetadata(Arguments.testDefaultsBuilder().setId(setId).build(), jsonSampleSource);
        when(jsonSampleSource.sample(SampleType.TUMOR)).thenReturn(tumorSample);
        when(jsonSampleSource.sample(SampleType.REFERENCE)).thenReturn(refSample);
        when(tumorSample.name()).thenReturn(tumorName);
        when(refSample.name()).thenReturn(refName);
    }

    @Test
    public void setsRunNameToSetIdCombinedWithRunId() {
        assertThat(victim.get().runName()).isEqualTo(format("%s-%s", setId, Arguments.testDefaults().runId().get()));
    }

    @Test
    public void returnsTumorFromJson() {
        SomaticRunMetadata metadata = victim.get();

        assertThat(metadata.maybeTumor().isPresent()).isTrue();
        assertThat(metadata.tumor().type()).isEqualTo(SampleType.TUMOR);
        assertThat(metadata.tumor().sampleName()).isEqualTo(tumorName);
        assertThat(metadata.tumor().sampleId()).isEqualTo(tumorName);
    }

    @Test
    public void returnsReferenceFromJson() {
        SomaticRunMetadata metadata = victim.get();

        assertThat(metadata.reference().type()).isEqualTo(SampleType.REFERENCE);
        assertThat(metadata.reference().sampleName()).isEqualTo(refName);
        assertThat(metadata.reference().sampleId()).isEqualTo(refName);
    }
}