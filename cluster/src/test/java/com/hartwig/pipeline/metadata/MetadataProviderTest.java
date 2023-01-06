package com.hartwig.pipeline.metadata;

import static java.lang.String.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;

import com.hartwig.pipeline.input.MetadataProvider;
import com.hartwig.pipeline.input.Sample;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.PipelineState;
import com.hartwig.pipeline.input.JsonPipelineInput;
import com.hartwig.pipeline.input.SingleSampleRunMetadata.SampleType;
import com.hartwig.pipeline.input.SomaticRunMetadata;
import com.hartwig.pipeline.output.OutputPublisher;

import org.junit.Before;
import org.junit.Test;

public class MetadataProviderTest {

    private SomaticMetadataApi victim;
    private String setId;
    private String tumorName;
    private String refName;
    private JsonPipelineInput jsonSampleSource;
    private Sample tumorSample;
    private OutputPublisher outputPublisher;

    @Before
    public void setUp() throws Exception {
        setId = "setId";
        tumorName = "CORE123T";
        refName = "CORE123R";
        jsonSampleSource = mock(JsonPipelineInput.class);
        tumorSample = mock(Sample.class);
        final Sample refSample = mock(Sample.class);
        outputPublisher = mock(OutputPublisher.class);
        victim = new MetadataProvider(Arguments.testDefaultsBuilder().setId(setId).build(),
                jsonSampleSource,
                () -> outputPublisher);
        when(jsonSampleSource.sample(SampleType.REFERENCE)).thenReturn(Optional.of(refSample));
        when(tumorSample.name()).thenReturn(tumorName);
        when(refSample.name()).thenReturn(refName);
    }

    @Test
    public void setsRunNameToSetIdCombinedWithRunId() {
        when(jsonSampleSource.sample(SampleType.TUMOR)).thenReturn(Optional.of(tumorSample));
        assertThat(victim.get().set()).isEqualTo(format("%s-%s", setId, Arguments.testDefaults().runTag().orElseThrow()));
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
        new MetadataProvider(Arguments.testDefaultsBuilder().setId(setId).publishDbLoadEvent(true).build(),
                jsonSampleSource,
                () -> outputPublisher).complete(state, metadata);
        verify(outputPublisher).publish(state, metadata);
    }

    @Test
    public void doesNotPublishesEventIfNotSpecifiedInArguments() {
        new MetadataProvider(Arguments.testDefaultsBuilder().setId(setId).publishDbLoadEvent(false).build(),
                jsonSampleSource,
                () -> outputPublisher).complete(mock(PipelineState.class), mock(SomaticRunMetadata.class));
        verify(outputPublisher, never()).publish(any(), any());
    }
}