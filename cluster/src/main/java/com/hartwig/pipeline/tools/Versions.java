package com.hartwig.pipeline.tools;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.stream.Stream;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface Versions {

    String BWA = "0.7.17";
    String SAMBAMBA = "0.6.8";
    String GATK = "3.8.0";
    String BCF_TOOLS = "1.9";
    String SAGE = "2.8";
    String SNPEFF = "4.3s";
    String TABIX = "0.2.6";
    String AMBER = "3.4";
    String COBALT = "1.11";
    String HEALTH_CHECKER = "3.2";
    String PURPLE = "3.1";
    String CIRCOS = "0.69.6";
    String GRIDSS = "2.11.1";
    String VIRUSBREAKEND_GRIDSS = "2.11.1";
    String VIRUS_INTERPRETER = "1.0";
    String GRIPSS = "1.11";
    String LINX = "1.16";
    String CHORD = "2.00_1.14";
    String SAMTOOLS = "1.10";
    String BAMCOMP = "1.3";
    String PROTECT = "1.4";
    String REPEAT_MASKER = "4.1.1";
    String KRAKEN = "2.1.0";
    String CUPPA = "1.4";
    String PEACH = "1.3";
    String SIGS = "1.0";
    String ORANGE = "1.1";

   static void printAll() {
        Logger logger = LoggerFactory.getLogger(Versions.class);
        logger.info("Version of pipeline5 is [{}] ", pipelineVersion());
        logger.info("Versions of tools used are [");
        Stream.of(Versions.class.getDeclaredFields())
                .filter(field -> Modifier.isStatic(field.getModifiers()))
                .map(Versions::format)
                .forEach(logger::info);
        logger.info("]");
    }

    static String pipelineVersion() {
        String version = Versions.class.getPackage().getImplementationVersion();
        return version != null ? version : "local-SNAPSHOT";
    }

    static String pipelineMajorMinorVersion() {
        return majorMinorVersion(pipelineVersion());
    }

    static String majorMinorVersion(final String version) {
        if (version != null) {
            String[] parts = version.split("\\.");
            if (parts.length == 3) {
                return String.format("%s.%s", parts[0], parts[1]);
            }
        }
        return version;
    }

    @NotNull
    static String format(final Field field) {
        try {
            return field.getName() + ": " + field.get(null);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    static String imageVersion() {
        return "5-24";
    }
}
