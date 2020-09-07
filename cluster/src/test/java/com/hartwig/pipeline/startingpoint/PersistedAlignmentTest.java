package com.hartwig.pipeline.startingpoint;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.alignment.AlignmentOutput;
import com.hartwig.pipeline.alignment.persisted.PersistedAlignment;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Test;

public class PersistedAlignmentTest {

    @Test
    public void returnsPersistedBams() {
        PersistedAlignment victim = new PersistedAlignment("bucket", "set", false);
        AlignmentOutput output = victim.run(TestInputs.referenceRunMetadata());
        assertThat(output.sample()).isEqualTo("reference");
        assertThat(output.status()).isEqualTo(PipelineStatus.PERSISTED);
        assertThat(output.finalBamLocation()).isEqualTo(GoogleStorageLocation.of("bucket", "set/reference/aligner/reference.bam"));
        assertThat(output.finalBaiLocation()).isEqualTo(GoogleStorageLocation.of("bucket", "set/reference/aligner/reference.bam.bai"));
    }

    @Test
    public void returnsPersistedCrams() {
        PersistedAlignment victim = new PersistedAlignment("bucket", "set", true);
        AlignmentOutput output = victim.run(TestInputs.referenceRunMetadata());
        assertThat(output.sample()).isEqualTo("reference");
        assertThat(output.status()).isEqualTo(PipelineStatus.PERSISTED);
        assertThat(output.finalBamLocation()).isEqualTo(GoogleStorageLocation.of("bucket", "set/reference/cram/reference.cram"));
        assertThat(output.finalBaiLocation()).isEqualTo(GoogleStorageLocation.of("bucket", "set/reference/cram/reference.cram.crai"));
    }
}