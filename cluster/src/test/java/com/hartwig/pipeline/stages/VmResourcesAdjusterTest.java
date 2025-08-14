package com.hartwig.pipeline.stages;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;

import com.hartwig.computeengine.execution.vm.BashStartupScript;
import com.hartwig.computeengine.storage.ResultsDirectory;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinitions;

import org.junit.Test;

public class VmResourcesAdjusterTest {

    @Test
    public void testOverrideCpus() {
        var arguments = mock(Arguments.class);
        when(arguments.stageCpusOverrideRegex()).thenReturn(Optional.of(".*"));
        when(arguments.stageCpusOverride()).thenReturn(Optional.of(8));
        when(arguments.stageMemoryOverrideRegex()).thenReturn(Optional.empty());
        when(arguments.stageMemoryOverrideGb()).thenReturn(Optional.empty());

        var vmResourcesAdjuster = new VmResourcesAdjuster(arguments);

        var peach = VirtualMachineJobDefinitions.peach(mock(BashStartupScript.class), mock(ResultsDirectory.class));
        assertEquals(2, peach.performanceProfile().cpus().getAsInt());
        assertEquals(4, peach.performanceProfile().memoryGB().getAsInt());

        var adjustedVmDefinition = vmResourcesAdjuster.overrideVmDefinition(peach);
        assertEquals(8, adjustedVmDefinition.performanceProfile().cpus().getAsInt());
        assertEquals(4, adjustedVmDefinition.performanceProfile().memoryGB().getAsInt());
    }

    @Test
    public void testOverrideMemory() {
        var arguments = mock(Arguments.class);
        when(arguments.stageCpusOverrideRegex()).thenReturn(Optional.empty());
        when(arguments.stageCpusOverride()).thenReturn(Optional.empty());
        when(arguments.stageMemoryOverrideRegex()).thenReturn(Optional.of(".*"));
        when(arguments.stageMemoryOverrideGb()).thenReturn(Optional.of(64));

        var vmResourcesAdjuster = new VmResourcesAdjuster(arguments);

        var peach = VirtualMachineJobDefinitions.peach(mock(BashStartupScript.class), mock(ResultsDirectory.class));
        assertEquals(2, peach.performanceProfile().cpus().getAsInt());
        assertEquals(4, peach.performanceProfile().memoryGB().getAsInt());

        var adjustedVmDefinition = vmResourcesAdjuster.overrideVmDefinition(peach);
        assertEquals(2, adjustedVmDefinition.performanceProfile().cpus().getAsInt());
        assertEquals(64, adjustedVmDefinition.performanceProfile().memoryGB().getAsInt());
    }

    @Test
    public void testOverrideCpusAndMemory() {
        var arguments = mock(Arguments.class);
        when(arguments.stageCpusOverrideRegex()).thenReturn(Optional.of(".*"));
        when(arguments.stageCpusOverride()).thenReturn(Optional.of(8));
        when(arguments.stageMemoryOverrideRegex()).thenReturn(Optional.of(".*"));
        when(arguments.stageMemoryOverrideGb()).thenReturn(Optional.of(64));

        var vmResourcesAdjuster = new VmResourcesAdjuster(arguments);

        var peach = VirtualMachineJobDefinitions.peach(mock(BashStartupScript.class), mock(ResultsDirectory.class));
        assertEquals(2, peach.performanceProfile().cpus().getAsInt());
        assertEquals(4, peach.performanceProfile().memoryGB().getAsInt());

        var adjustedVmDefinition = vmResourcesAdjuster.overrideVmDefinition(peach);
        assertEquals(8, adjustedVmDefinition.performanceProfile().cpus().getAsInt());
        assertEquals(64, adjustedVmDefinition.performanceProfile().memoryGB().getAsInt());
    }
}