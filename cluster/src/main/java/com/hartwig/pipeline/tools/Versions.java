package com.hartwig.pipeline.tools;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.stream.Stream;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface Versions {

    // external tools
    String BAMCOMP = "1.3";
    String BCF_TOOLS = "1.9";
    String BWA = "0.7.17";
    String CIRCOS = "0.69.6";
    String GATK = "3.8.0";
    String KRAKEN = "2.1.0";
    String REPEAT_MASKER = "4.1.1";
    String SAMBAMBA = "0.6.8";
    String SAMTOOLS = "1.14";
    String TABIX = "0.2.6";

    static void printAll() {
        Logger logger = LoggerFactory.getLogger(Versions.class);
        logger.info("Version of pipeline5 is [{}] ", pipelineVersion());
        logger.info("Versions of HMF tools used are [");

        for(ToolInfo tool : ToolInfo.values())
        {
            logger.info(String.format("%s: %s", tool.toString(), tool.runVersion()));
        }

        logger.info("Versions of external tools used are [");
        Stream.of(Versions.class.getDeclaredFields())
                .filter(field -> Modifier.isStatic(field.getModifiers()))
                .map(Versions::format)
                .forEach(logger::info);
        logger.info("]");
    }

    static String pipelineVersion() {
        String version = Versions.class.getPackage().getImplementationVersion();
        return version != null ? version : imageVersion().replace("-", ".");
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
        return "5-33";
    }

    static void main(final String[] args) {
        System.out.println(Versions.imageVersion());
    }
}
