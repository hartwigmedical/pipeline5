package com.hartwig.pipeline.calling.somatic;

import com.hartwig.pipeline.execution.vm.JavaClassCommand;

class SageCommand extends JavaClassCommand {
    SageCommand(final String mainClass, final String... arguments) {
        super("sage", "pilot", "sage.jar", mainClass, "8G", arguments);
    }
}
