package com.hartwig.pipeline.tools;

import static java.lang.String.format;

import com.hartwig.computeengine.execution.vm.VmDirectories;

public enum ExternalTool {
    BAMCOMP("bamcomp", "bamcomp.jar", "1.3"),
    BCF_TOOLS("bcftools", "bcftools", "1.9"),
    BWA("bwa", "bwa", "0.7.17"),
    CIRCOS("circos", "bin/circos", "0.69.6"),
    GATK("gatk", "GenomeAnalysisTK.jar", "3.8.0"),
    KRAKEN("kraken2", "", "2.1.0"),
    REPEAT_MASKER("repeatmasker", "", "4.1.1"),
    SAMBAMBA("sambamba", "sambamba", "0.6.8"),
    SAMTOOLS("samtools", "samtools", "1.14"),
    TABIX("tabix", "tabix", "0.2.6"),
    BLASTN("ncbi-blast", "bin/blastn", "2.14.1");

    private final String toolName;
    private final String version;
    private final String binary;

    ExternalTool(final String toolName, final String binary, final String version) {
        this.toolName = toolName;
        this.version = version;
        this.binary = binary;
    }

    public String path() {
        return format("%s/%s/%s", VmDirectories.TOOLS, toolName, version);
    }

    public String binaryPath() {
        return format("%s/%s/%s/%s", VmDirectories.TOOLS, toolName, version, binary);
    }

    public String getToolName() {
        return toolName;
    }

    public String getVersion() {
        return version;
    }

    public String getBinary() {
        return binary;
    }
}
