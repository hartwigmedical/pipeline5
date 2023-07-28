package com.hartwig.pipeline.tools;

import static java.lang.String.format;

import com.hartwig.pipeline.execution.vm.VmDirectories;

public enum HmfTool {

    AMBER("3.9", 32, 64, 16, false),
    BAM_TOOLS("1.1.rc1", 16, 24, 16, false), // 1.0 -> 1.1
    CHORD("2.02_1.14"),
    COBALT("1.15.2", 20, 24, 16, false),
    CUPPA("1.8.1.rc1"),
    GRIDSS("2.13.2"),
    GRIPSS("2.3.5", 16, 24, 1, false), // 2.3.5 -> 2.4
    HEALTH_CHECKER("3.5.rc1"), // 3.4 -> 3.5
    LILAC("1.5.rc1", 16, 24, 8, false), // 1.4.2 -> 1.5
    LINX("1.24.rc1", 8, 16, 1, false), // 1.23.6 -> 1.24
    MARK_DUPS("1.0.rc1", 16, 24, 16, false), // initial version
    ORANGE("2.6.0.rc1", 16, 18, 4, false), // 2.5.0 -> 2.5.2
    PAVE("1.5.rc1", 16, 24, 1, false), // 1.4.5 -> 1.5
    PEACH("1.7"),
    PURPLE("3.9.rc1", 24, 32, 8, false), // 3.8.4 -> 3.9
    SAGE("3.3.rc1", 60, 64, 16, false), // 3.2.5 -> 3.3
    SIGS("1.1"),
    SV_PREP("1.2", 48, 64, 24, false),
    VIRUSBREAKEND_GRIDSS("2.13.2"),
    VIRUS_INTERPRETER("1.3.rc1");

    private static final int DEFAULT_MAX_HEAP = 4;
    private static final int DEFAULT_MEMORY = 8;

    public static final String PILOT_VERSION = "pilot"; // will pick up the jar from /opt/toolName/pilot/toolName.jar

    private final String toolName;
    private final String version;
    private final int maxHeap;
    private final int memoryGb;
    private final int cpus;
    private final boolean usePilot;

    HmfTool(final String version) {
        this(version, DEFAULT_MAX_HEAP, DEFAULT_MEMORY, 1, false);
    }

    HmfTool(final String version, final int maxHeap, final int memoryGb, final int cpus, final boolean usePilot) {
        toolName = this.toString().toLowerCase().replace('_', '-');
        this.version = version;
        this.maxHeap = maxHeap;
        this.memoryGb = memoryGb;
        this.cpus = cpus;
        this.usePilot = usePilot;
    }

    public String getToolName() {
        return toolName;
    }

    public String getVersion() {
        return version;
    }

    public int getMaxHeap() {
        return maxHeap;
    }

    public int getMemoryGb() {
        return memoryGb;
    }

    public int getCpus() {
        return cpus;
    }

    public String runVersion() {
        return usePilot ? PILOT_VERSION : version;
    }

    public String directory() {
        return toolName;
    }

    public String jar() {
        return format("%s.jar", toolName);
    }

    public String maxHeapStr() {
        return format("%dG", maxHeap);
    }

    public String jarPath() {
        return format("%s/%s/%s/%s", VmDirectories.TOOLS, directory(), version, jar());
    }
}
