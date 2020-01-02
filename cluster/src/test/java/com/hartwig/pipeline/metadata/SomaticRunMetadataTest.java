package com.hartwig.pipeline.metadata;

import static com.hartwig.pipeline.testsupport.TestInputs.defaultSomaticRunMetadata;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;

import com.hartwig.pipeline.jackson.ObjectMappers;

import org.junit.Test;

public class SomaticRunMetadataTest {

    @Test
    public void serializeToJsonBothTumorAndReference() throws Exception {
        SomaticRunMetadata victim = defaultSomaticRunMetadata();
        assertThat(ObjectMappers.get().writeValueAsString(victim)).isEqualTo(
                "{\"runName\":\"run\",\"reference\":{\"sampleName\":\"reference\",\"sampleId\":\"reference\",\"type\":\"REFERENCE\"},"
                        + "\"tumor\":{\"sampleName\":\"tumor\",\"sampleId\":\"tumor\",\"type\":\"TUMOR\"}}");
    }

    @Test
    public void serializeToJsonSingleSample() throws Exception {
        SomaticRunMetadata victim =
                SomaticRunMetadata.builder().from(defaultSomaticRunMetadata()).maybeTumor(Optional.empty()).build();
        assertThat(ObjectMappers.get().writeValueAsString(victim)).isEqualTo("{\"runName\":\"run\",\"reference\":{\"sampleName\":"
                + "\"reference\",\"sampleId\":\"reference\",\"type\":\"REFERENCE\"},\"tumor\":null}");
    }

    @Test
    public void returnsSingleSampleWhenTumorIsNotSet() {
        SomaticRunMetadata victim = SomaticRunMetadata.builder().from(defaultSomaticRunMetadata()).maybeTumor(Optional.empty()).build();
        assertThat(victim.isSingleSample()).isTrue();
    }

    @Test
    public void returnsNotSingleSampleWhenTumorIsPresent() {
        SomaticRunMetadata victim = defaultSomaticRunMetadata();
        assertThat(victim.isSingleSample()).isFalse();
    }
}