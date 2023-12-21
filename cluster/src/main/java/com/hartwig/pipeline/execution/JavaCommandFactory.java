package com.hartwig.pipeline.execution;

import java.util.Collections;
import java.util.List;

import com.hartwig.computeengine.execution.vm.command.java.JavaClassCommand;
import com.hartwig.computeengine.execution.vm.command.java.JavaJarCommand;
import com.hartwig.pipeline.tools.ExternalTool;
import com.hartwig.pipeline.tools.HmfTool;

public final class JavaCommandFactory {

    private JavaCommandFactory() {
    }

    public static JavaJarCommand javaJarCommand(HmfTool hmfTool, List<String> arguments) {
        return new JavaJarCommand(hmfTool.getToolName(), hmfTool.getVersion(), hmfTool.jar(), hmfTool.maxHeapStr(), arguments);
    }

    public static JavaClassCommand javaClassCommand(HmfTool hmfTool, String mainClass, List<String> arguments) {
        return new JavaClassCommand(hmfTool.getToolName(),
                hmfTool.getVersion(),
                hmfTool.jar(),
                mainClass,
                hmfTool.maxHeapStr(),
                Collections.emptyList(),
                arguments);
    }

    public static JavaClassCommand javaClassCommand(ExternalTool externalTool, String mainClass, String heapSize, List<String> arguments) {
        return new JavaClassCommand(externalTool.getToolName(),
                externalTool.getVersion(),
                externalTool.getBinary(),
                mainClass,
                heapSize,
                Collections.emptyList(),
                arguments);
    }
}
