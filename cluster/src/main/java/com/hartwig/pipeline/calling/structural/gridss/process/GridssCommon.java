package com.hartwig.pipeline.calling.structural.gridss.process;

import com.hartwig.pipeline.execution.vm.JavaClassCommand;
import com.hartwig.pipeline.execution.vm.VmDirectories;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;

public class GridssCommon {
    static final int GRIDSS_BWA_BASES_PER_BATCH = 40000000;

    static JavaClassCommand gridssCommand(String className, String memory, String... gridsArguments) {
        return gridssCommand(className, memory, Collections.emptyList(), gridsArguments);
    }

    static JavaClassCommand gridssCommand(String className, String memory, List<String> additionalJvmArguments, String... gridsArguments) {
        List<String> jvmArgs = new ArrayList<>(asList(
                "-ea",
                "-Dsamjdk.create_index=true",
                "-Dsamjdk.use_async_io_read_samtools=true",
                "-Dsamjdk.use_async_io_write_samtools=true",
                "-Dsamjdk.use_async_io_write_tribble=true"));
        jvmArgs.addAll(additionalJvmArguments);

        return new JavaClassCommand("gridss",
                "2.2.2",
                "gridss.jar",
                className,
                memory,
                jvmArgs,
                gridsArguments);
    }

    // TODO
    static String pathToBwa() {
        return String.format("%s/bwa/0.7.17/bwa", VmDirectories.TOOLS);
    }

    static JavaClassCommand gridssCommand(String className, String memory, List<String> additionalJvmArguments, List<String> gridssArguments) {
        return gridssCommand(className, memory, additionalJvmArguments, gridssArguments.toArray(new String[] {}));
    }

    static String configFile() {
        return VmDirectories.outputFile("gridss.properties");
    }
}
