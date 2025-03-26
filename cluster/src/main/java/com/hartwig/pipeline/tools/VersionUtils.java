package com.hartwig.pipeline.tools;

import static java.util.Collections.emptyList;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class VersionUtils {

    public static void printAll() {
        Logger logger = LoggerFactory.getLogger(VersionUtils.class);
        logger.info("Version of pipeline5 is [{}] ", pipelineVersion());

        logger.info("HMF tool versions:");

        for (HmfTool tool : HmfTool.values()) {
            logger.info("    {}: {}", tool, tool.versionInfo());
        }

        logger.info("External tools versions:");
        for (ExternalTool tool : ExternalTool.values()) {
            logger.info("    {}: {}", tool, tool.getVersion());
        }

        Stream.of(VersionUtils.class.getDeclaredFields())
                .filter(field -> Modifier.isStatic(field.getModifiers()))
                .map(VersionUtils::format)
                .forEach(logger::info);
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

    public static List<HmfTool> inArtifactRegistry() {
        return emptyList();
    }

    public static String format(final Field field) {
        try {
            return field.getName() + ": " + field.get(null);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public static String imageVersion() {
        return "6-0";
    }

    public static void main(final String[] args) {
        if (args.length != 0 && args[0].equals("tools")) {
            VersionUtils.inArtifactRegistry().forEach(t -> System.out.printf("%s %s\n", t.getToolName(), t.runVersion()));
        } else {
            System.out.println(VersionUtils.imageVersion());
        }
    }
}
