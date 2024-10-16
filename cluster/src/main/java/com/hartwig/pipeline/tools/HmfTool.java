package com.hartwig.pipeline.tools;

import static java.lang.String.format;

import com.hartwig.computeengine.execution.vm.VmDirectories;

public enum HmfTool {

    AMBER("4.1rc", 24, 16, false), // 4.0.1 -> 4.1
    BAM_TOOLS("1.3rc", 24, 16, false), // 1.2.1 -> 1.3
    CHORD("2.1.0rc", 12, 4, false), // 2.02_1.14 ->
    CIDER("1.0.3", 24, 4, false),
    COBALT("2.0rc", 24, 16, false), // 1.16 -> 2.0
    CUPPA("2.3rc", 16, 4, false), // 2.1.1 -> 2.3
    ESVEE("1.0rc", 96, 16, false),
    HEALTH_CHECKER("3.6rc", 32, 8, false), // 3.5 -> 3.6
    LILAC("1.6", 24, 8, false),
    LINX("2.0rc", 12, 4, false), // 1.25 -> 2.0
    REDUX("1.0rc", 128, 16, false), // MarkDups 1.1.7 -> Redux 1.0
    ORANGE("3.7.1rc", 16, 4, false), // 3.5.1 -> 3.7.1
    PAVE("1.7rc", 40, 8, false), // 1.6 -> 1.7
    PEACH("2.0.0", 4, 2, false, 50),
    PURPLE("4.1rc", 40, 8, false), // 4.0.2 -> 4.1
    SAGE("4.0rc", 64, 16, false), // 3.4.3 -> 4.0
    SIGS("1.2.1", 16, 4, false, 30),
    TEAL("1.3.2rc", 32, 16, false), // 1.2.2 -> 1.3.2
    VCHORD("0.1", 4, 2, false),
    VIRUSBREAKEND_GRIDSS("2.13.3", 64, 8, false),
    VIRUS_INTERPRETER("1.6rc", 8, 2, false); // 1.3 -> 1.6

    private static final String PILOT_VERSION = "pilot"; // will pick up the jar from /opt/toolName/pilot/toolName.jar
    private final String toolName;
    private final String version;
    private final int memoryGb;
    private final int cpus;
    private final boolean usePilot;
    private final int maxHeapPercentage;

    HmfTool(final String version, final int memoryGb, final int cpus, final boolean usePilot, final int maxHeapPercentage) {
        toolName = this.toString().toLowerCase().replace('_', '-');
        this.version = version;
        this.memoryGb = memoryGb;
        this.cpus = cpus;
        this.usePilot = usePilot;
        this.maxHeapPercentage = maxHeapPercentage;
    }

    HmfTool(final String version, final int memoryGb, final int cpus, final boolean usePilot) {
        this(version, memoryGb, cpus, usePilot, Defaults.JAVA_HEAP_PERCENTAGE);
    }

    public String getToolName() {
        return toolName;
    }

    public String runVersion() {
        return usePilot ? PILOT_VERSION : version;
    }

    public String versionInfo() {
        return usePilot ? format("%s, using pilot", version) : version;
    }

    public int getMaxHeapPercentage() {
        return maxHeapPercentage;
    }

    public int getMemoryGb() {
        return memoryGb;
    }

    public int getCpus() {
        return cpus;
    }

    public String directory() {
        return toolName;
    }

    public String jar() {
        return format("%s.jar", toolName);
    }

    public String jarPath() {
        return format("%s/%s/%s/%s", VmDirectories.TOOLS, directory(), version, jar());
    }

    private static final class Defaults {
        private static final int JAVA_HEAP_PERCENTAGE = 90;
    }
}
