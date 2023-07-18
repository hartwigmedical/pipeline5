package com.hartwig.pipeline.tools;

import static java.lang.String.format;

import com.hartwig.pipeline.execution.vm.VmDirectories;

public enum ToolInfo {

    AMBER("3.9", 32, 64, 16, false),
    BAM_TOOLS("1.1.rc1", 16, 24, 16, false), // 1.0 -> 1.1
    CHORD("2.02_1.14"),
    COBALT("1.15.rc1", 16, 24, 16, false),
    CUPPA("1.8.1.rc1"),
    GRIDSS("2.13.2"),
    GRIPSS("2.3.5", 16, 24, 1, false), // 2.3.5 -> 2.4
    HEALTH_CHECKER("3.5.rc1"),
    LILAC("1.5.rc1", 16, 24, 8, false), // 1.4.2 -> 1.5
    LINX("1.24.rc1", 8, 16, 1, false), // 1.23.6 -> 1.24
    MARK_DUPS("1.0.rc1", 16, 24, 16, false), // initial version
    ORANGE("2.5.2.rc1", 16, 18, 4, false), // 2.5.0 -> 2.5.2
    PAVE("1.5.rc1", 16, 24, 1, false), // 1.4.5 -> 1.5
    PEACH("1.7"),
    PURPLE("3.9.rc1", 24, 32, 8, false), // 3.8.4 -> 3.9
    SAGE("3.3.rc1", 60, 64, 16, false), // 3.2.5 -> 3.3
    SIGS("1.1"),
    SV_PREP("1.2", 48, 64, 24, false),
    VIRUSBREAKEND_GRIDSS("2.13.2"),
    VIRUS_INTERPRETER("1.2");

    public static final int DEFAULT_MAX_HEAP = 4;
    public static final int DEFAULT_MEMORY = 8;

    public static final String PILOT_VERSION = "pilot"; // will pick up the jar from /opt/toolName/pilot/toolName.jar

    public final String ToolName;
    public final String Version;
    public final int MaxHeap;
    public final int MemoryGb;
    public final int CPUs;
    public final boolean UsePilot;

    ToolInfo(final String version) {
        this(version, DEFAULT_MAX_HEAP, DEFAULT_MEMORY, 1, false);
    }

    ToolInfo(final String version, int maxHeap, int memoryGb, int cpus, boolean usePilot) {
        ToolName = this.toString().toLowerCase().replace('_', '-');
        Version = version;
        MaxHeap = maxHeap;
        MemoryGb = memoryGb;
        CPUs = cpus;
        UsePilot = usePilot;
    }

    public String runVersion() { return UsePilot ? PILOT_VERSION : Version; }
    public String directory() { return ToolName; }

    public String jar() { return format("%s.jar", ToolName); }
    public String maxHeapStr() { return format("%dG", MaxHeap); }

    public String jarPath() { return format("%s/%s/%s/%s", VmDirectories.TOOLS, directory(), Version, jar()); }
}
