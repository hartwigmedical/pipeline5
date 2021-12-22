package com.hartwig.pipeline.calling.structural.gridss.command;

import com.hartwig.pipeline.execution.vm.java.JavaClassCommand;
import com.hartwig.pipeline.tools.Versions;

class GripssKtCommand extends JavaClassCommand
{
    public GripssKtCommand(final String mainClass, final String... arguments) {
        super("gripss", Versions.GRIPSS, "gripss.jar", mainClass, "24G", arguments); }
}
