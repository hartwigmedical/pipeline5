package com.hartwig.pipeline.tools;

import static java.lang.String.format;

import com.hartwig.computeengine.execution.vm.VmDirectories;

public enum HmfTool {

    AMBER("4.1.1", 24, 16, false),
    BAM_TOOLS("1.3", 24, 16, false),
    CHORD("2.1.0", 24, 4, false),
    CIDER("1.0.3", 24, 4, false),
    COBALT("2.0", 24, 16, false),
    CUPPA("2.3.0", 16, 4, false),
    ESVEE("1.0.2", 64, 32, false),
    HEALTH_CHECKER("3.6", 32, 8, false),
    LILAC("1.6", 24, 16, false),
    LINX("2.0.2", 16, 4, false),
    REDUX("1.1.2", 64, 32, false),
    ORANGE("3.7.1", 16, 4, false),
    PAVE("1.7.1", 32, 8, false),
    PEACH("2.0.0", 4, 2, false, 50),
    PURPLE("4.1", 40, 8, false),
    SAGE("4.0", 64, 32, false),
    SIGS("1.2.1", 16, 4, false, 30),
    TEAL("1.3.3", 32, 32, false),
    V_CHORD("1.0", 4, 2, false),
    VIRUSBREAKEND_GRIDSS("2.13.3", 64, 16, false),
    VIRUS_INTERPRETER("1.7", 8, 2, false);

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
