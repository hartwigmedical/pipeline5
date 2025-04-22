package com.hartwig.pipeline.stages;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;

import com.hartwig.computeengine.execution.vm.BashStartupScript;
import com.hartwig.computeengine.storage.ResultsDirectory;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinitions;

import org.junit.Test;

public class VmMemoryAdjusterTest {

    @Test
    public void testOverrideVmDefinition() {
        var arguments = mock(Arguments.class);
        when(arguments.stageMemoryOverrideRegex()).thenReturn(Optional.of(".*"));
        when(arguments.stageMemoryOverrideGb()).thenReturn(Optional.of(64));

        var vmMemoryAdjuster = new VmMemoryAdjuster(arguments);

        var peach = VirtualMachineJobDefinitions.peach(mock(BashStartupScript.class), mock(ResultsDirectory.class));
        assertEquals(4, peach.performanceProfile().memoryGB().getAsInt());

        var adjustedVmDefinition = vmMemoryAdjuster.overrideVmDefinition(peach);
        assertEquals(64, adjustedVmDefinition.performanceProfile().memoryGB().getAsInt());
    }

    @Test
    public void testOverrideRegexDoesNotMatch() {
        var arguments = mock(Arguments.class);
        when(arguments.stageMemoryOverrideRegex()).thenReturn(Optional.of("esvee"));
        when(arguments.stageMemoryOverrideGb()).thenReturn(Optional.of(64));

        var vmMemoryAdjuster = new VmMemoryAdjuster(arguments);

        var peach = VirtualMachineJobDefinitions.peach(mock(BashStartupScript.class), mock(ResultsDirectory.class));
        assertEquals(4, peach.performanceProfile().memoryGB().getAsInt());

        var adjustedVmDefinition = vmMemoryAdjuster.overrideVmDefinition(peach);
        assertEquals(4, adjustedVmDefinition.performanceProfile().memoryGB().getAsInt());
    }

    @Test
    public void testOverridesNotSet() {
        var arguments = mock(Arguments.class);
        when(arguments.stageMemoryOverrideRegex()).thenReturn(Optional.empty());
        when(arguments.stageMemoryOverrideGb()).thenReturn(Optional.empty());

        var vmMemoryAdjuster = new VmMemoryAdjuster(arguments);

        var peach = VirtualMachineJobDefinitions.peach(mock(BashStartupScript.class), mock(ResultsDirectory.class));
        var adjustedVmDefinition = vmMemoryAdjuster.overrideVmDefinition(peach);
        assertEquals(4, adjustedVmDefinition.performanceProfile().memoryGB().getAsInt());
    }

    @Test
    public void testRegexNotSet() {
        var arguments = mock(Arguments.class);
        when(arguments.stageMemoryOverrideRegex()).thenReturn(Optional.empty());
        when(arguments.stageMemoryOverrideGb()).thenReturn(Optional.of(64));

        var vmMemoryAdjuster = new VmMemoryAdjuster(arguments);

        var peach = VirtualMachineJobDefinitions.peach(mock(BashStartupScript.class), mock(ResultsDirectory.class));
        var adjustedVmDefinition = vmMemoryAdjuster.overrideVmDefinition(peach);
        assertEquals(4, adjustedVmDefinition.performanceProfile().memoryGB().getAsInt());
    }

    @Test
    public void testMemoryValueNotSet() {
        var arguments = mock(Arguments.class);
        when(arguments.stageMemoryOverrideRegex()).thenReturn(Optional.of(".*"));
        when(arguments.stageMemoryOverrideGb()).thenReturn(Optional.empty());

        var vmMemoryAdjuster = new VmMemoryAdjuster(arguments);

        var peach = VirtualMachineJobDefinitions.peach(mock(BashStartupScript.class), mock(ResultsDirectory.class));
        var adjustedVmDefinition = vmMemoryAdjuster.overrideVmDefinition(peach);
        assertEquals(4, adjustedVmDefinition.performanceProfile().memoryGB().getAsInt());
    }
}