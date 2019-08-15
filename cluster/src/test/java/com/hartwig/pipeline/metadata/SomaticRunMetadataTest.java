package com.hartwig.pipeline.metadata;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;

import com.hartwig.pipeline.sbpapi.ObjectMappers;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Test;

public class SomaticRunMetadataTest {

    @Test
    public void serializeToJsonBothTumorAndReference() throws Exception {
        SomaticRunMetadata victim = TestInputs.defaultSomaticRunMetadata();
        assertThat(ObjectMappers.get().writeValueAsString(victim)).isEqualTo(
                "{\"runName\":\"run\",\"reference\":{\"sampleName\":\"reference\",\"sampleId\":\"reference\",\"type\":\"REFERENCE\"},"
                        + "\"tumor\":{\"sampleName\":\"tumor\",\"sampleId\":\"tumor\",\"type\":\"TUMOR\"}}");
    }

    @Test
    public void serializeToJsonSingleSample() throws Exception {
        SomaticRunMetadata victim =
                SomaticRunMetadata.builder().from(TestInputs.defaultSomaticRunMetadata()).maybeTumor(Optional.empty()).build();
        assertThat(ObjectMappers.get().writeValueAsString(victim)).isEqualTo("{\"runName\":\"run\",\"reference\":{\"sampleName\":"
                + "\"reference\",\"sampleId\":\"reference\",\"type\":\"REFERENCE\"},\"tumor\":null}");
    }
}