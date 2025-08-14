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

public class VmCpusAdjusterTest {

    @Test
    public void testOverrideVmDefinition() {
        var arguments = mock(Arguments.class);
        when(arguments.stageCpusOverrideRegex()).thenReturn(Optional.of(".*"));
        when(arguments.stageCpusOverride()).thenReturn(Optional.of(4));

        var vmCpusAdjuster = new VmCpusAdjuster(arguments);

        var peach = VirtualMachineJobDefinitions.peach(mock(BashStartupScript.class), mock(ResultsDirectory.class));
        assertEquals(2, peach.performanceProfile().cpus().getAsInt());

        var adjustedVmDefinition = vmCpusAdjuster.overrideVmDefinition(peach);
        assertEquals(4, adjustedVmDefinition.performanceProfile().cpus().getAsInt());
    }

    @Test
    public void testOverrideRegexDoesNotMatch() {
        var arguments = mock(Arguments.class);
        when(arguments.stageCpusOverrideRegex()).thenReturn(Optional.of("esvee"));
        when(arguments.stageCpusOverride()).thenReturn(Optional.of(4));

        var vmCpusAdjuster = new VmCpusAdjuster(arguments);

        var peach = VirtualMachineJobDefinitions.peach(mock(BashStartupScript.class), mock(ResultsDirectory.class));
        assertEquals(2, peach.performanceProfile().cpus().getAsInt());

        var adjustedVmDefinition = vmCpusAdjuster.overrideVmDefinition(peach);
        assertEquals(2, adjustedVmDefinition.performanceProfile().cpus().getAsInt());
    }

    @Test
    public void testOverridesNotSet() {
        var arguments = mock(Arguments.class);
        when(arguments.stageCpusOverrideRegex()).thenReturn(Optional.empty());
        when(arguments.stageCpusOverride()).thenReturn(Optional.empty());

        var vmCpusAdjuster = new VmCpusAdjuster(arguments);

        var peach = VirtualMachineJobDefinitions.peach(mock(BashStartupScript.class), mock(ResultsDirectory.class));
        var adjustedVmDefinition = vmCpusAdjuster.overrideVmDefinition(peach);
        assertEquals(2, adjustedVmDefinition.performanceProfile().cpus().getAsInt());
    }

    @Test
    public void testRegexNotSet() {
        var arguments = mock(Arguments.class);
        when(arguments.stageCpusOverrideRegex()).thenReturn(Optional.empty());
        when(arguments.stageCpusOverride()).thenReturn(Optional.of(4));

        var vmCpusAdjuster = new VmCpusAdjuster(arguments);

        var peach = VirtualMachineJobDefinitions.peach(mock(BashStartupScript.class), mock(ResultsDirectory.class));
        var adjustedVmDefinition = vmCpusAdjuster.overrideVmDefinition(peach);
        assertEquals(2, adjustedVmDefinition.performanceProfile().cpus().getAsInt());
    }

    @Test
    public void testCpusNotSet() {
        var arguments = mock(Arguments.class);
        when(arguments.stageCpusOverrideRegex()).thenReturn(Optional.of(".*"));
        when(arguments.stageCpusOverride()).thenReturn(Optional.empty());

        var vmCpusAdjuster = new VmCpusAdjuster(arguments);

        var peach = VirtualMachineJobDefinitions.peach(mock(BashStartupScript.class), mock(ResultsDirectory.class));
        var adjustedVmDefinition = vmCpusAdjuster.overrideVmDefinition(peach);
        assertEquals(2, adjustedVmDefinition.performanceProfile().cpus().getAsInt());
    }
}