package com.hartwig.pipeline.testsupport;

public class Resources {

    public static String testResource(final String name) {
        return System.getProperty("user.dir") + "/src/test/resources/" + name;
    }

    public static String targetResource(final String name) {
        return System.getProperty("user.dir") + "/target/" + name;
    }
}
