package com.hartwig.pipeline.execution;

import java.util.List;

import com.hartwig.computeengine.execution.vm.command.java.JavaJarCommand;
import com.hartwig.pipeline.tools.HmfTool;

public final class JavaCommandFactory {

    private JavaCommandFactory() {
    }

    public static JavaJarCommand javaJarCommand(HmfTool hmfTool, List<String> arguments) {
        return new JavaJarCommand(hmfTool.getToolName(), hmfTool.getVersion(), hmfTool.jar(), hmfTool.maxHeapStr(), arguments);
    }
}
