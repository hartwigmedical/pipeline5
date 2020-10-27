package com.hartwig.pipeline.alignment.persisted;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;

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

    public static final GoogleStorageLocation PERSISTED_REFERENCE_BAM = GoogleStorageLocation.of("bucket", "persisted/reference.bam");

    @Test
    public void returnsPersistedWhenExists() {
        PersistedDataset persistedDataset = mock(PersistedDataset.class);
        when(persistedDataset.path(TestInputs.referenceRunMetadata().sampleName(), DataType.ALIGNED_READS)).thenReturn(Optional.of(
                PERSISTED_REFERENCE_BAM));
        PersistedAlignment victim = new PersistedAlignment(persistedDataset);
        AlignmentOutput output = victim.run(TestInputs.referenceRunMetadata());
        assertThat(output.sample()).isEqualTo("reference");
        assertThat(output.status()).isEqualTo(PipelineStatus.PERSISTED);
        assertThat(output.finalBamLocation()).isEqualTo(PERSISTED_REFERENCE_BAM);
        assertThat(output.finalBaiLocation()).isEqualTo(GoogleStorageLocation.of(PERSISTED_REFERENCE_BAM.bucket(),
                FileTypes.bai(PERSISTED_REFERENCE_BAM.path())));
    }

    @Test
    public void returnsBamsInConventionalLocationIfNoPersisted() {
        PersistedAlignment victim = new PersistedAlignment(new NoopPersistedDataset());
        AlignmentOutput output = victim.run(TestInputs.referenceRunMetadata());
        assertThat(output.sample()).isEqualTo("reference");
        assertThat(output.status()).isEqualTo(PipelineStatus.PERSISTED);
        assertThat(output.finalBamLocation()).isEqualTo(GoogleStorageLocation.of("bucket", "set/reference/aligner/reference.bam"));
        assertThat(output.finalBaiLocation()).isEqualTo(GoogleStorageLocation.of("bucket", "set/reference/aligner/reference.bam.bai"));
    }
}