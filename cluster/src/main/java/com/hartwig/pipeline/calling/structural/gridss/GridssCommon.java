package com.hartwig.pipeline.calling.structural.gridss;

import com.hartwig.pipeline.execution.vm.VmDirectories;

import static java.lang.String.format;

public class GridssCommon {
    public static final int GRIDSS_BWA_BASES_PER_BATCH = 40000000;

    public static String pathToBwa() {
        return format("%s/bwa/0.7.17/bwa", VmDirectories.TOOLS);
    }
    public static String pathToSamtools() {
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
