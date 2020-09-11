package com.hartwig.pipeline.alignment.persisted;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;

import com.hartwig.pipeline.alignment.AlignmentOutput;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Test;

public class PersistedAlignmentTest {

    @Test
    public void returnsPersistedWhenExists() {
        PersistedAlignment victim = new PersistedAlignment((m, r) -> Optional.of("persisted/reference.bam"));
        AlignmentOutput output = victim.run(TestInputs.referenceRunMetadata());
        assertThat(output.sample()).isEqualTo("reference");
        assertThat(output.status()).isEqualTo(PipelineStatus.PERSISTED);
        assertThat(output.finalBamLocation()).isEqualTo(GoogleStorageLocation.of("bucket", "persisted/reference.bam"));
        assertThat(output.finalBaiLocation()).isEqualTo(GoogleStorageLocation.of("bucket", "persisted/reference.bam.bai"));
    }

    @Test
    public void returnsBamsInConventionalLocationIfNoPersisted() {
        PersistedAlignment victim = new PersistedAlignment((m, r) -> Optional.empty());
        AlignmentOutput output = victim.run(TestInputs.referenceRunMetadata());
        assertThat(output.sample()).isEqualTo("reference");
        assertThat(output.status()).isEqualTo(PipelineStatus.PERSISTED);
        assertThat(output.finalBamLocation()).isEqualTo(GoogleStorageLocation.of("bucket", "set/reference/aligner/reference.bam"));
        assertThat(output.finalBaiLocation()).isEqualTo(GoogleStorageLocation.of("bucket", "set/reference/aligner/reference.bam.bai"));
    }
}