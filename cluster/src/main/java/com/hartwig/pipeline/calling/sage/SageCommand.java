package com.hartwig.pipeline.calling.sage;

import com.hartwig.pipeline.execution.vm.JavaClassCommand;
import com.hartwig.pipeline.tools.Versions;

public class SageCommand extends JavaClassCommand {
    public SageCommand(final String mainClass, final String maxHeapSize, final String... arguments) {
        super("sage", Versions.SAGE, "sage.jar", mainClass, maxHeapSize, arguments);
    }
}
