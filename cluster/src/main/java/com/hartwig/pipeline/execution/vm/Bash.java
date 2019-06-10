package com.hartwig.pipeline.execution.vm;

public class Bash {

    public static String allCpus() {
        return "$(grep -c '^processor' /proc/cpuinfo)";
    }
}
