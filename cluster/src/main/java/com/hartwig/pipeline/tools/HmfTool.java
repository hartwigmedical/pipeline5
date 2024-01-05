package com.hartwig.pipeline.tools;

import static java.lang.String.format;

import com.hartwig.pipeline.execution.vm.VmDirectories;

public enum HmfTool {

    AMBER("4.0rc", 20, 24, 16, false), // 3.9 -> 4.0
    BAM_TOOLS("1.2rc", 16, 24, 16, false), // 1.1 -> 1.2
    CHORD("2.02_1.14", Defaults.JAVA_HEAP, 12, 4, false),
    CIDER("1.0.2", 12, 16, 4, false),
    COBALT("1.16rc", 20, 24, 16, false), // 1.15 -> 1.16
    CUPPA("1.8.1", Defaults.JAVA_HEAP, 16, 4, false),
    GRIDSS("2.13.2", Defaults.JAVA_HEAP, 64, 24, false),
    GRIPSS("2.4rc", 16, 24, 4, false), // 2.3.5 -> 2.4
    HEALTH_CHECKER("3.5", Defaults.JAVA_HEAP, 32, 8, false),
    LILAC("1.5.2", 16, 24, 8, false),
    LINX("1.25rc", 8, 12, 4, false), // 1.24.1 -> 1.25
    MARK_DUPS("1.1rc", 40, 64, 24, false), // 1.1 -> 1.2
    ORANGE("3.0.2", 16, 18, 4, false),
    PAVE("1.6rc", 16, 24, 8, false), // 1.5 -> 1.6
    PEACH("1.7"),
    PURPLE("4.0rc", 31, 39, 8, false), // 3.9.2 -> 4.0
    SAGE("3.4rc", 48, 64, 24, false), // 3.3 -> 3.4
    SIGS("1.1", Defaults.JAVA_HEAP, 16, 4, false),
    SV_PREP("1.2.3rc", 48, 64, 24, false), // 1.2 -> 1.2.3
    TEAL("1.1.0", 30, 32, 16, false),
    VIRUSBREAKEND_GRIDSS("2.13.2", Defaults.JAVA_HEAP, 64, 12, false),
    VIRUS_INTERPRETER("1.3", Defaults.JAVA_HEAP, 8, 2, false);

    private final String toolName;
    private final String version;
    private final int maxHeap;
    private final int memoryGb;
    private final int cpus;
    private final boolean usePilot;

    HmfTool(final String version) {
        this(version, Defaults.JAVA_HEAP, Defaults.MEMORY, Defaults.CORES, false);
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
    public String runVersion() {
        return usePilot ? Defaults.PILOT_VERSION : version;
    }

    public int getMaxHeap() {
        return maxHeap;
    }
    public int getMemoryGb() { return memoryGb; }
    public int getCpus() {
        return cpus;
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

    private static final class Defaults {
        public static final String PILOT_VERSION = "pilot"; // will pick up the jar from /opt/toolName/pilot/toolName.jar
        private static final int CORES = 1;
        private static final int JAVA_HEAP = 4;
        private static final int MEMORY = 8;
    }
}
