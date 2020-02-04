package com.hartwig.pipeline.calling.somatic;

import com.hartwig.pipeline.execution.vm.JavaClassCommand;
import com.hartwig.pipeline.tools.Versions;

class SageCommand extends JavaClassCommand {
    SageCommand(final String mainClass, final String maxHeapSize, final String... arguments) {
        super("sage", Versions.SAGE, "sage.jar", mainClass, maxHeapSize, arguments);
    }
}
