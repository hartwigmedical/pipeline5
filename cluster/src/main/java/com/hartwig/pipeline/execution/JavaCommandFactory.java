package com.hartwig.pipeline.execution;

import com.hartwig.computeengine.execution.vm.command.java.JavaJarCommand;
import com.hartwig.pipeline.tools.HmfTool;

import java.util.List;

public final class JavaCommandFactory {

    private JavaCommandFactory() {
    }

    public static JavaJarCommand javaJarCommand(HmfTool hmfTool, List<String> arguments) {
        return new JavaJarCommand(hmfTool.getToolName(), hmfTool.getVersion(), hmfTool.jar(), hmfTool.maxHeapStr(), arguments);
    }
}
