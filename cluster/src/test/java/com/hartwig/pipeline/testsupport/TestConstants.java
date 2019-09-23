package com.hartwig.pipeline.testsupport;

import com.hartwig.pipeline.tools.Versions;

import static java.lang.String.format;

public class TestConstants {
    public static final String OUT_DIR = "/data/output";
    public static final String RESOURCE_DIR = "/data/resources";
    public static final String TOOLS_DIR = "/opt/tools";
    public static final String IN_DIR = "/data/input";
    public static final String LOG_FILE = "/var/log/run.log";
    public static final String PROC_COUNT = "$(grep -c '^processor' /proc/cpuinfo)";

    public static final String TOOLS_AMBER_JAR = tool("amber/" + Versions.AMBER + "/amber.jar");
    public static final String TOOLS_BWA_DIR = TOOLS_DIR + "/bwa/" + Versions.BWA;
    public static final String TOOLS_BWA = TOOLS_BWA_DIR + "/bwa";
    public static final String TOOLS_BGZIP = tool("tabix/" + Versions.TABIX + "/bgzip");
    public static final String TOOLS_STRELKA_POSTPROCESS = tool("strelka-post-process/"
            + Versions.STRELKA_POST_PROCESS + "/strelka-post-process.jar");
    public static final String TOOLS_SAMBAMBA = tool("sambamba/" + Versions.SAMBAMBA + "/sambamba");
    public static final String TOOLS_SNPEFF_DIR = tool("snpEff/4.3s");
    public static final String TOOLS_BCFTOOLS = tool("bcftools/" + Versions.BCF_TOOLS + "/bcftools");
    public static final String TOOLS_PURPLE_JAR = tool("purple/" + Versions.PURPLE + "/purple.jar");
    public static final String TOOLS_COBALT_JAR = tool("cobalt/" + Versions.COBALT + "/cobalt.jar");
    public static final String TOOLS_SAGE_JAR = tool("sage/" + Versions.SAGE + "/sage.jar");
    public static final String TOOLS_TABIX = tool("tabix/" + Versions.TABIX + "/tabix");
    public static final String TOOLS_HEALTHCHECKER_JAR = tool("health-checker/" + Versions.HEALTH_CHECKER + "/health-checker.jar");
    public static final String TOOLS_GRIDSS_JAR = tool("gridss/" + Versions.GRIDSS + "/gridss.jar");
    public static final String TOOLS_GATK_JAR = tool("gatk/" + Versions.GATK + "/GenomeAnalysisTK.jar");

    private static String tool(String specificPath) {
        return format("%s/%s", TOOLS_DIR, specificPath);
    }

    public static String outFile(String filename) {
        return OUT_DIR + "/" + filename;
    }

    public static String inFile(String filename) {
        return IN_DIR + "/" + filename;
    }

    public static String resource(String filename) {
        return RESOURCE_DIR + "/" + filename;
    }

    public static String copyOutputToStorage(String destination) {
        return "gsutil -qm -o GSUtil:parallel_composite_upload_threshold=150M cp -r " + OUT_DIR + "/ " + destination;
    }

    public static String copyInputToLocal(String source, String destination) {
        return format("gsutil -qm cp -n %s %s/%s", source, IN_DIR, destination);
    }
}