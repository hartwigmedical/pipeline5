package com.hartwig.pipeline.execution.vm.java;

import com.hartwig.pipeline.execution.vm.BashCommand;

public class JavaJarFileExistsCommand implements BashCommand {

    private final JavaJarCommand javaJarCommand;
    private final String argument;
    private final String targetPath;

    public JavaJarFileExistsCommand(JavaJarCommand javaJarCommand, String argument, String targetPath) {
        this.javaJarCommand = javaJarCommand;
        this.argument = argument;
        this.targetPath = targetPath;
    }

    @Override
    public String asBash() {
        return String.format("if [ -e %s ]; then %s %s %s ; else %s ; fi",
                targetPath,
                javaJarCommand.asBash(),
                argument,
                targetPath,
                javaJarCommand.asBash());
    }
}
