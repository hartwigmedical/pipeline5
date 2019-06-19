package com.hartwig.pipeline.metadata;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;

import com.hartwig.pipeline.Arguments;

import org.junit.Test;

public class SampleMetadataApiProviderTest {

    @Test
    public void createsLocalWhenNoSbpSampleId() {
        SampleMetadataApiProvider victim =
                SampleMetadataApiProvider.from(Arguments.testDefaultsBuilder().sbpApiSampleId(Optional.empty()).build());
        assertThat(victim.get()).isInstanceOf(LocalSampleMetadataApi.class);
    }

    @Test
    public void createsSbpWhenSbpSampleId() {
        SampleMetadataApiProvider victim =
                SampleMetadataApiProvider.from(Arguments.testDefaultsBuilder().sbpApiSampleId(Optional.of(1)).build());
        assertThat(victim.get()).isInstanceOf(SbpSampleMetadataApi.class);
    }
}