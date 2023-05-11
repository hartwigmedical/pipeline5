package com.hartwig.pipeline.input;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import com.hartwig.pdl.OperationalReferences;
import com.hartwig.pdl.PipelineInput;
import com.hartwig.pdl.SampleInput;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ImmutableArguments;
import com.hartwig.pipeline.input.SingleSampleRunMetadata.SampleType;

import org.junit.Before;
import org.junit.Test;

public class MetadataProviderTest {
    private static final String EXPECTED_PDL_RUN_TAG = "pdlSet-test";
    private PipelineInput pipelineInput;
    private ImmutableArguments arguments;
    private MetadataProvider victim;

    @Before
    public void setup() {
        pipelineInput = PipelineInput.builder().setName("pdlSet").build();
        arguments = Arguments.testDefaultsBuilder().setName("argSet").build();
    }

    @Test
    public void shouldSetRunTagFromPdlSetNameWhenGiven() {
        victim = new MetadataProvider(arguments, pipelineInput);
        assertThat(victim.get().set()).isEqualTo(EXPECTED_PDL_RUN_TAG);
    }

    @Test
    public void shouldSetRunTagFromArgumentsWhenNotInPdl() {
        pipelineInput = PipelineInput.builder().build();
        victim = new MetadataProvider(arguments, pipelineInput);
        assertThat(victim.get().set()).isEqualTo("argSet-test");
    }

    @Test
    public void shouldSetOutputBucketFromArguments() {
        String outputBucket = "my-output-bucket";
        victim = new MetadataProvider(Arguments.testDefaultsBuilder().outputBucket(outputBucket).build(), pipelineInput);
        assertThat(victim.get().bucket()).isEqualTo(outputBucket);
    }

    @Test
    public void shouldSetOperationalReferencesFromPdlWhenGiven() {
        long runId = 1L;
        long setId = 42L;
        OperationalReferences setReferences = OperationalReferences.builder().runId(runId).setId(setId).build();
        pipelineInput = PipelineInput.builder().operationalReferences(setReferences).build();
        victim = new MetadataProvider(arguments, pipelineInput);
        assertThat(victim.get().maybeExternalIds()).isPresent();
        OperationalReferences foundReferences = victim.get().maybeExternalIds().orElseThrow();
        assertThat(foundReferences.runId()).isEqualTo(runId);
        assertThat(foundReferences.setId()).isEqualTo(setId);
    }

    @Test
    public void shouldSetReferenceSampleWhenGiven() {
        String sampleName = "refSample";
        String barcode = "FR1234";
        SampleInput reference = SampleInput.builder().name(sampleName).barcode(barcode).primaryTumorDoids(List.of("a", "b")).build();
        pipelineInput = PipelineInput.builder().from(pipelineInput).reference(reference).build();
        victim = new MetadataProvider(arguments, pipelineInput);
        assertThat(victim.get().maybeReference()).isPresent();
        SingleSampleRunMetadata referenceMetadata = victim.get().reference();
        assertThat(referenceMetadata.sampleName()).isEqualTo(sampleName);
        assertThat(referenceMetadata.type()).isEqualTo(SampleType.REFERENCE);
        assertThat(referenceMetadata.barcode()).isEqualTo(barcode);
        assertThat(referenceMetadata.set()).isEqualTo(EXPECTED_PDL_RUN_TAG);
        assertThat(referenceMetadata.bucket()).isEqualTo(arguments.outputBucket());
        assertThat(referenceMetadata.primaryTumorDoids()).isEmpty();
    }

    @Test
    public void shouldSetTumorSampleFromPdlWhenGiven() {
        String sampleName = "tumorSample";
        String barcode = "FR1235";
        List<String> doids = List.of("a", "b", "c");
        SampleInput tumor = SampleInput.builder().name(sampleName).barcode(barcode).primaryTumorDoids(doids).build();
        pipelineInput = PipelineInput.builder().from(pipelineInput).tumor(tumor).build();
        victim = new MetadataProvider(arguments, pipelineInput);
        assertThat(victim.get().maybeTumor()).isPresent();
        SingleSampleRunMetadata tumorMetadata = victim.get().tumor();
        assertThat(tumorMetadata.sampleName()).isEqualTo(sampleName);
        assertThat(tumorMetadata.type()).isEqualTo(SampleType.TUMOR);
        assertThat(tumorMetadata.barcode()).isEqualTo(barcode);
        assertThat(tumorMetadata.set()).isEqualTo(EXPECTED_PDL_RUN_TAG);
        assertThat(tumorMetadata.bucket()).isEqualTo(arguments.outputBucket());
        assertThat(tumorMetadata.primaryTumorDoids()).isEqualTo(doids);
    }

    @Test
    public void shouldUseSampleNameAsBarcodeIfBarcodeNotSet() {
        String sampleName = "tumorSample";
        List<String> doids = List.of("a", "b", "c");
        SampleInput tumor = SampleInput.builder().name(sampleName).primaryTumorDoids(doids).build();
        pipelineInput = PipelineInput.builder().from(pipelineInput).tumor(tumor).build();
        victim = new MetadataProvider(arguments, pipelineInput);
        assertThat(victim.get().maybeTumor()).isPresent();
        SingleSampleRunMetadata tumorMetadata = victim.get().tumor();
        assertThat(tumorMetadata.barcode()).isEqualTo(sampleName);
    }
}