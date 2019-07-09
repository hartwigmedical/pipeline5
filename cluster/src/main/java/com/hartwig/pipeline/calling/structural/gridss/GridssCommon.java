package com.hartwig.pipeline.calling.structural.gridss;

import static java.lang.String.format;

import java.io.File;

import com.hartwig.pipeline.execution.vm.VmDirectories;

public class GridssCommon {
    public static String pathToBwa() {
        return format("%s/bwa/0.7.17/bwa", VmDirectories.TOOLS);
    }
    public static String pathToGridssScripts() {
        return format("%s/gridss-scripts/4.8.1", VmDirectories.TOOLS);
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

    public static String basenameNoExtensions(final String completeFilename) {
        return new File(completeFilename).getName().split("\\.")[0];
    }
}
