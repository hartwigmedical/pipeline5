package com.hartwig.pipeline.alignment.persisted;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.alignment.AlignmentOutput;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.reruns.NoopPersistedDataset;
import com.hartwig.pipeline.reruns.PersistedDataset;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Test;

public class PersistedAlignmentTest {

    public static final GoogleStorageLocation PERSISTED_REFERENCE_CRAM = GoogleStorageLocation.of("bucket", "persisted/reference.cram");

    @Test
    public void returnsPersistedCramsWhenExists() {
        PersistedDataset persistedDataset = mock(PersistedDataset.class);
        when(persistedDataset.path(TestInputs.referenceRunMetadata().sampleName(), DataType.ALIGNED_READS)).thenReturn(Optional.of(
                PERSISTED_REFERENCE_CRAM));
        PersistedAlignment victim = new PersistedAlignment(persistedDataset, Arguments.testDefaults());
        AlignmentOutput output = victim.run(TestInputs.referenceRunMetadata());
        assertThat(output.sample()).isEqualTo("reference");
        assertThat(output.status()).isEqualTo(PipelineStatus.PERSISTED);
        assertThat(output.alignments()).isEqualTo(PERSISTED_REFERENCE_CRAM);
    }

    @Test
    public void returnsBamsInConventionalLocationIfNoPersisted() {
        PersistedAlignment victim = new PersistedAlignment(new NoopPersistedDataset(), Arguments.testDefaults());
        AlignmentOutput output = victim.run(TestInputs.referenceRunMetadata());
        assertThat(output.sample()).isEqualTo("reference");
        assertThat(output.status()).isEqualTo(PipelineStatus.PERSISTED);
        assertThat(output.alignments()).isEqualTo(GoogleStorageLocation.of("bucket", "set/reference/aligner/reference.bam"));
    }

    @Test
    public void returnsCramsInConventionalLocationIfNoPersistedAndUseCrams() {
        PersistedAlignment victim =
                new PersistedAlignment(new NoopPersistedDataset(), Arguments.testDefaultsBuilder().useCrams(true).build());
        AlignmentOutput output = victim.run(TestInputs.referenceRunMetadata());
        assertThat(output.sample()).isEqualTo("reference");
        assertThat(output.status()).isEqualTo(PipelineStatus.PERSISTED);
        assertThat(output.alignments()).isEqualTo(GoogleStorageLocation.of("bucket", "set/reference/cram/reference.cram"));
    }
}