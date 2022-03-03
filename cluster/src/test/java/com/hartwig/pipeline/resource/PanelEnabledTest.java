package com.hartwig.pipeline.resource;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.mock;

import org.junit.Test;

public class PanelEnabledTest {

    @Test
    public void returnsLocalPathToOverridenBedFileGCS() {
        ResourceFiles decorated = mock(ResourceFiles.class);
        PanelEnabled victim = new PanelEnabled(decorated, "gs://bucket/panel/panel.bed");
        assertThat(victim.panelBed()).hasValue("/opt/resources/panel/override/panel.bed");
    }

    @Test
    public void returnsLocalPathToBedFileInImage() {
        ResourceFiles decorated = mock(ResourceFiles.class);
        PanelEnabled victim = new PanelEnabled(decorated, "panel.bed");
        assertThat(victim.panelBed()).hasValue("/opt/resources/panel/panel.bed");
    }
}