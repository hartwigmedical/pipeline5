package com.hartwig.pipeline.resource;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.mock;

import org.junit.Test;

public class TargetRegionsEnabledTest {

    @Test
    public void returnsLocalPathToOverridenBedFileGCS() {
        ResourceFiles decorated = mock(ResourceFiles.class);
        TargetRegionsEnabled victim = new TargetRegionsEnabled(decorated, "gs://bucket/target_regions/target_regions.bed");
        assertThat(victim.targetRegionsBed()).hasValue("/opt/resources/target_regions/override/target_regions.bed");
    }

    @Test
    public void returnsLocalPathToBedFileInImage() {
        ResourceFiles decorated = mock(ResourceFiles.class);
        TargetRegionsEnabled victim = new TargetRegionsEnabled(decorated, "target_regions.bed");
        assertThat(victim.targetRegionsBed()).hasValue("/opt/resources/target_regions/target_regions.bed");
    }
}