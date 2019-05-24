package com.hartwig.pipeline.calling.structural.gridss.process;

import com.hartwig.pipeline.execution.vm.JavaClassCommand;
import com.hartwig.pipeline.execution.vm.VmDirectories;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.lang.String.format;
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

    static String pathToBwa() {
        return format("%s/bwa/0.7.17/bwa", VmDirectories.TOOLS);
    }
    static String pathToSamtools() {
        return format("%s/samtools/1.2/samtools", VmDirectories.TOOLS);
    }
    public static String pathToGridssScripts() {
        return format("%s/gridss-scripts/4.8", VmDirectories.TOOLS);
    }

    public static String configFile() {
        return format("%s/gridss.properties", VmDirectories.RESOURCES);
    }

    public static String blacklist() {
        return format("%s/ENCFF001TDO.bed", VmDirectories.RESOURCES);
    }

    public static String tmpDir() {
        return "/tmp";
    }

    public static String ponDir() {
        return format("%s/gridss_pon", VmDirectories.RESOURCES);
    }
}
