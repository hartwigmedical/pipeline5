package com.hartwig.pipeline.metadata;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.util.Optional;

import com.hartwig.patient.ImmutableReferenceTumorPair;
import com.hartwig.patient.ImmutableSample;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Test;

public class ModeResolverTest {

    @Test
    public void bothTumorAndReferenceReturnsSomatic() {
        ModeResolver victim = new ModeResolver();
        final InputMode result = victim.apply(TestInputs.defaultSomaticRunMetadata());
        assertThat(result).isEqualTo(InputMode.SOMATIC);
    }

    @Test
    public void referenceOnlyReturnsGermlineOnly() {
        ModeResolver victim = new ModeResolver();
        final InputMode result = victim.apply(ImmutableSomaticRunMetadata.builder()
                .from(TestInputs.defaultSomaticRunMetadata())
                .maybeTumor(Optional.empty())
                .build());
        assertThat(result).isEqualTo(InputMode.GERMLINE_ONLY);
    }

    @Test
    public void tumorOnlyReturnsTumorOnly() {
        ModeResolver victim = new ModeResolver();
        final InputMode result = victim.apply(ImmutableSomaticRunMetadata.builder()
                .from(TestInputs.defaultSomaticRunMetadata())
                .maybeReference(Optional.empty())
                .build());
        assertThat(result).isEqualTo(InputMode.TUMOR_ONLY);
    }

}