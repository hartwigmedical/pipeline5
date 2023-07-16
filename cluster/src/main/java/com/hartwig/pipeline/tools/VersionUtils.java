package com.hartwig.pipeline.tools;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class VersionUtils
{

    public static void printAll() {
        Logger logger = LoggerFactory.getLogger(VersionUtils.class);
        logger.info("Version of pipeline5 is [{}] ", pipelineVersion());

        logger.info("HMF tool versions:");

        for(ToolInfo tool : ToolInfo.values())
        {
            logger.info(String.format("%s: %s", tool.toString(), tool.runVersion()));
        }

        logger.info("External tools versions:");
        for(ExternalTool tool : ExternalTool.values())
        {
            logger.info(String.format("%s: %s", tool.toString(), tool.Version));
        }

        Stream.of(VersionUtils.class.getDeclaredFields())
                .filter(field -> Modifier.isStatic(field.getModifiers()))
                .map(VersionUtils::format)
                .forEach(logger::info);
        logger.info("]");
    }

    public static String pipelineVersion() {
        String version = VersionUtils.class.getPackage().getImplementationVersion();
        return version != null ? version : imageVersion().replace("-", ".");
    }

    public static String pipelineMajorMinorVersion() {
        return majorMinorVersion(pipelineVersion());
    }

    public static String majorMinorVersion(final String version) {
        if (version != null) {
            String[] parts = version.split("\\.");
            if (parts.length == 3) {
                return String.format("%s.%s", parts[0], parts[1]);
            }
        }
        return version;
    }

    public static String format(final Field field) {
        try {
            return field.getName() + ": " + field.get(null);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public static String imageVersion() {
        return "5-33";
    }

    public static void main(final String[] args) {
        System.out.println(VersionUtils.imageVersion());
    }
}
