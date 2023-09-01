package com.hartwig.pipeline.alignment.persisted;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.PipelineStatus;
import com.hartwig.computeengine.storage.GoogleStorageLocation;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.alignment.AlignmentOutput;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.reruns.PersistedDataset;
import com.hartwig.pipeline.stages.TestPersistedDataset;
import com.hartwig.pipeline.testsupport.TestBlobs;
import com.hartwig.pipeline.testsupport.TestInputs;
import org.junit.Before;
import org.junit.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PersistedAlignmentTest {

    private static final GoogleStorageLocation PERSISTED_REFERENCE_CRAM =
            GoogleStorageLocation.of(TestInputs.BUCKET, "set/reference/cram/reference.cram");
    private static final GoogleStorageLocation PERSISTED_REFERENCE_BAM =
            GoogleStorageLocation.of(TestInputs.BUCKET, "set/reference/aligner/reference.bam");
    private Storage storage;

    @Before
    public void setUp() throws Exception {
        storage = mock(Storage.class);
    }

    @Test
    public void returnsPersistedCramsWhenExists() {
        PersistedDataset persistedDataset = mock(PersistedDataset.class);
        when(persistedDataset.path(TestInputs.referenceRunMetadata().sampleName(), DataType.ALIGNED_READS)).thenReturn(Optional.of(
                PERSISTED_REFERENCE_CRAM));
        PersistedAlignment victim = new PersistedAlignment(persistedDataset, Arguments.testDefaults(), storage);
        AlignmentOutput output = victim.run(TestInputs.referenceRunMetadata());
        assertThat(output.sample()).isEqualTo("reference");
        assertThat(output.status()).isEqualTo(PipelineStatus.PERSISTED);
        assertThat(output.alignments()).isEqualTo(PERSISTED_REFERENCE_CRAM);
    }

    @Test
    public void returnsBamsInConventionalLocationIfNotPersistedAndTheyExist() {
        final Blob blob = TestBlobs.blob(PERSISTED_REFERENCE_BAM.path());
        final Page<Blob> page = TestBlobs.pageOf(blob);
        when(storage.list(TestInputs.BUCKET, Storage.BlobListOption.prefix(PERSISTED_REFERENCE_BAM.path()))).thenReturn(page);
        PersistedAlignment victim = new PersistedAlignment(new TestPersistedDataset(), Arguments.testDefaults(), storage);
        AlignmentOutput output = victim.run(TestInputs.referenceRunMetadata());
        assertThat(output.sample()).isEqualTo("reference");
        assertThat(output.status()).isEqualTo(PipelineStatus.PERSISTED);
        assertThat(output.alignments()).isEqualTo(PERSISTED_REFERENCE_BAM);
    }

    @Test
    public void returnsCramsInConventionalLocationIfNotPersistedAndBamsDontExist() {
        final Page<Blob> page = TestBlobs.pageOf();
        when(storage.list(TestInputs.BUCKET, Storage.BlobListOption.prefix(PERSISTED_REFERENCE_CRAM.path()))).thenReturn(page);
        PersistedAlignment victim = new PersistedAlignment(new TestPersistedDataset(), Arguments.testDefaults(), storage);
        AlignmentOutput output = victim.run(TestInputs.referenceRunMetadata());
        assertThat(output.sample()).isEqualTo("reference");
        assertThat(output.status()).isEqualTo(PipelineStatus.PERSISTED);
        assertThat(output.alignments()).isEqualTo(PERSISTED_REFERENCE_CRAM);
    }

    @Test
    public void returnsCramsInConventionalLocationIfNoPersistedAndUseCrams() {
        PersistedAlignment victim =
                new PersistedAlignment(new TestPersistedDataset(), Arguments.testDefaultsBuilder().useCrams(true).build(), storage);
        AlignmentOutput output = victim.run(TestInputs.referenceRunMetadata());
        assertThat(output.sample()).isEqualTo("reference");
        assertThat(output.status()).isEqualTo(PipelineStatus.PERSISTED);
        assertThat(output.alignments()).isEqualTo(PERSISTED_REFERENCE_CRAM);
    }
}