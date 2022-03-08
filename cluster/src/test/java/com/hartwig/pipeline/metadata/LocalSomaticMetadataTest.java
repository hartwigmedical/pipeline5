package com.hartwig.pipeline.metadata;

import static java.lang.String.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;

import com.hartwig.patient.Sample;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.PipelineState;
import com.hartwig.pipeline.alignment.sample.JsonSampleSource;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata.SampleType;
import com.hartwig.pipeline.transfer.staged.StagedOutputPublisher;

import org.junit.Before;
import org.junit.Test;

public class LocalSomaticMetadataTest {

    private SomaticMetadataApi victim;
    private String setId;
    private String tumorName;
    private String refName;
    private JsonSampleSource jsonSampleSource;
    private Sample tumorSample;
    private StagedOutputPublisher stagedOutputPublisher;

    @Before
    public void setUp() throws Exception {
        setId = "setId";
        tumorName = "CORE123T";
        refName = "CORE123R";
        jsonSampleSource = mock(JsonSampleSource.class);
        tumorSample = mock(Sample.class);
        final Sample refSample = mock(Sample.class);
        stagedOutputPublisher = mock(StagedOutputPublisher.class);
        victim = new LocalSomaticMetadata(Arguments.testDefaultsBuilder().setId(setId).build(), jsonSampleSource, stagedOutputPublisher);
        when(jsonSampleSource.sample(SampleType.REFERENCE)).thenReturn(Optional.of(refSample));
        when(tumorSample.name()).thenReturn(tumorName);
        when(refSample.name()).thenReturn(refName);
    }

    @Test
    public void setsRunNameToSetIdCombinedWithRunId() {
        when(jsonSampleSource.sample(SampleType.TUMOR)).thenReturn(Optional.of(tumorSample));
        assertThat(victim.get().set()).isEqualTo(format("%s-%s", setId, Arguments.testDefaults().runId().orElseThrow()));
    }

    @Test
    public void returnsTumorFromJson() {
        when(jsonSampleSource.sample(SampleType.TUMOR)).thenReturn(Optional.of(tumorSample));
        SomaticRunMetadata metadata = victim.get();

        assertThat(metadata.maybeTumor().isPresent()).isTrue();
        assertThat(metadata.tumor().type()).isEqualTo(SampleType.TUMOR);
        assertThat(metadata.tumor().sampleName()).isEqualTo(tumorName);
        assertThat(metadata.tumor().barcode()).isEqualTo(tumorName);
    }

    @Test
    public void returnsReferenceFromJson() {
        when(jsonSampleSource.sample(SampleType.TUMOR)).thenReturn(Optional.of(tumorSample));
        SomaticRunMetadata metadata = victim.get();

        assertThat(metadata.reference().type()).isEqualTo(SampleType.REFERENCE);
        assertThat(metadata.reference().sampleName()).isEqualTo(refName);
        assertThat(metadata.reference().barcode()).isEqualTo(refName);
    }

    @Test
    public void supportsSingleSample() {
        when(jsonSampleSource.sample(SampleType.TUMOR)).thenReturn(Optional.empty());
        SomaticRunMetadata metadata = victim.get();

        assertThat(metadata.reference().type()).isEqualTo(SampleType.REFERENCE);
        assertThat(metadata.reference().sampleName()).isEqualTo(refName);
        assertThat(metadata.reference().barcode()).isEqualTo(refName);

        assertThat(metadata.maybeTumor()).isEmpty();
    }

    @Test
    public void publishesEventIfSpecifiedInArguments() {
        PipelineState state = mock(PipelineState.class);
        SomaticRunMetadata metadata = mock(SomaticRunMetadata.class);
        new LocalSomaticMetadata(Arguments.testDefaultsBuilder().setId(setId).publishLoadEvent(true).build(),
                jsonSampleSource, stagedOutputPublisher).complete(state, metadata);
        verify(stagedOutputPublisher).publish(state, metadata);
    }

    @Test
    public void doesNotPublishesEventIfNotSpecifiedInArguments() {
        new LocalSomaticMetadata(Arguments.testDefaultsBuilder().setId(setId).publishLoadEvent(false).build(),
                jsonSampleSource, stagedOutputPublisher).complete(mock(PipelineState.class), mock(SomaticRunMetadata.class));
        verify(stagedOutputPublisher, never()).publish(any(), any());
    }
}